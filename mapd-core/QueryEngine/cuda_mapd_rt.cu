#include <stdio.h>
#include <stdint.h>
#include <float.h>
#include <cuda.h>
#include <limits>
#include "BufferCompaction.h"
#include "ExtensionFunctions.hpp"
#include "GpuRtConstants.h"
#include "HyperLogLogRank.h"

extern "C" __device__ int32_t pos_start_impl(const int32_t* row_index_resume) {
  return blockIdx.x * blockDim.x + threadIdx.x;
}

extern "C" __device__ int32_t group_buff_idx_impl() {
  return pos_start_impl(NULL);
}

extern "C" __device__ int32_t pos_step_impl() {
  return blockDim.x * gridDim.x;
}

extern "C" __device__ int8_t thread_warp_idx(const int8_t warp_sz) {
  return threadIdx.x % warp_sz;
}

extern "C" __device__ const int64_t* init_shared_mem_nop(const int64_t* groups_buffer,
                                                         const int32_t groups_buffer_size) {
  return groups_buffer;
}

extern "C" __device__ void write_back_nop(int64_t* dest, int64_t* src, const int32_t sz) {}

extern "C" __device__ const int64_t* init_shared_mem(const int64_t* groups_buffer, const int32_t groups_buffer_size) {
  extern __shared__ int64_t fast_bins[];
  if (threadIdx.x == 0) {
    memcpy(fast_bins, groups_buffer, groups_buffer_size);
  }
  __syncthreads();
  return fast_bins;
}

extern "C" __device__ void write_back(int64_t* dest, int64_t* src, const int32_t sz) {
  __syncthreads();
  if (threadIdx.x == 0) {
    memcpy(dest, src, sz);
  }
}

#define init_group_by_buffer_gpu_impl init_group_by_buffer_gpu

#include "GpuInitGroups.cu"

#undef init_group_by_buffer_gpu_impl

// Dynamic watchdog: monitoring up to 64 SMs. E.g. GP100 config may have 60:
// 6 Graphics Processing Clusters (GPCs) * 10 Streaming Multiprocessors
__device__ int64_t dw_sm_cycle_start[64];  // Set from host before launching the kernel
__device__ int64_t dw_cycle_budget = 0;    // Set from host before launching the kernel
__device__ int32_t dw_abort = 0;           // TBD: set from host (async)

__inline__ __device__ uint32_t get_smid(void) {
  uint32_t ret;
  asm("mov.u32 %0, %%smid;" : "=r"(ret));
  return ret;
}

extern "C" __device__ bool dynamic_watchdog() {
  if (dw_cycle_budget == 0LL)
    return false;  // Uninitialized watchdog can't check time
  if (dw_abort == 1)
    return true;  // Received host request to abort
  uint32_t smid = get_smid();
  if (smid >= 64)
    return false;
  __shared__ int64_t dw_block_cycle_start;  // Thread block shared cycle start
  int64_t cycle_count = static_cast<int64_t>(clock64());
  if (threadIdx.x == 0) {
    dw_block_cycle_start = 0LL;
    // Make sure the block hasn't switched SMs
    if (smid == get_smid()) {
      dw_block_cycle_start =
          static_cast<int64_t>(atomicCAS(reinterpret_cast<unsigned long long*>(&dw_sm_cycle_start[smid]),
                                         0ULL,
                                         static_cast<unsigned long long>(cycle_count)));
    }
  }
  __syncthreads();
  if (dw_block_cycle_start > 0LL) {
    if (smid == get_smid()) {
      // Check if we're out of time on this particular SM
      int64_t cycles = cycle_count - dw_block_cycle_start;
      return (cycles > dw_cycle_budget);
    }
  }
  return false;
}

template <typename T = unsigned long long>
inline __device__ T get_empty_key() {
  return EMPTY_KEY_64;
}

template <>
inline __device__ unsigned int get_empty_key() {
  return EMPTY_KEY_32;
}

template <typename T>
inline __device__ int64_t* get_matching_group_value(int64_t* groups_buffer,
                                                    const uint32_t h,
                                                    const T* key,
                                                    const uint32_t key_count,
                                                    const uint32_t row_size_quad) {
  const T empty_key = get_empty_key<T>();
  uint32_t off = h * row_size_quad;
  auto row_ptr = reinterpret_cast<T*>(groups_buffer + off);
  {
    const T old = atomicCAS(row_ptr, empty_key, *key);
    if (empty_key == old && key_count > 1) {
      for (size_t i = 1; i <= key_count - 1; ++i) {
        atomicExch(row_ptr + i, key[i]);
      }
    }
  }
  if (key_count > 1) {
    while (atomicAdd(row_ptr + key_count - 1, 0) == empty_key) {
      // spin until the winning thread has finished writing the entire key and the init value
    }
  }
  bool match = true;
  for (uint32_t i = 0; i < key_count; ++i) {
    if (row_ptr[i] != key[i]) {
      match = false;
      break;
    }
  }

  if (match) {
    auto row_ptr_i8 = reinterpret_cast<int8_t*>(row_ptr + key_count);
    return reinterpret_cast<int64_t*>(align_to_int64(row_ptr_i8));
  }
  return NULL;
}

extern "C" __device__ int64_t* get_matching_group_value(int64_t* groups_buffer,
                                                        const uint32_t h,
                                                        const int64_t* key,
                                                        const uint32_t key_count,
                                                        const uint32_t key_width,
                                                        const uint32_t row_size_quad,
                                                        const int64_t* init_vals) {
  switch (key_width) {
    case 4:
      return get_matching_group_value(
          groups_buffer, h, reinterpret_cast<const unsigned int*>(key), key_count, row_size_quad);
    case 8:
      return get_matching_group_value(
          groups_buffer, h, reinterpret_cast<const unsigned long long*>(key), key_count, row_size_quad);
    default:
      return NULL;
  }
}

extern "C" __device__ int64_t* get_matching_group_value_columnar(int64_t* groups_buffer,
                                                                 const uint32_t h,
                                                                 const int64_t* key,
                                                                 const uint32_t key_qw_count,
                                                                 const size_t entry_count) {
  uint32_t off = h;
  {
    const uint64_t old = atomicCAS(reinterpret_cast<unsigned long long*>(groups_buffer + off), EMPTY_KEY_64, *key);
    if (EMPTY_KEY_64 == old) {
      for (size_t i = 0; i < key_qw_count; ++i) {
        groups_buffer[off] = key[i];
        off += entry_count;
      }
      return &groups_buffer[off];
    }
  }
  __syncthreads();
  off = h;
  for (size_t i = 0; i < key_qw_count; ++i) {
    if (groups_buffer[off] != key[i]) {
      return NULL;
    }
    off += entry_count;
  }
  return &groups_buffer[off];
}

#include "MurmurHash.cpp"
#include "GroupByRuntime.cpp"
#include "JoinHashTableQueryRuntime.cpp"
#include "TopKRuntime.cpp"

__device__ int64_t atomicMax64(int64_t* address, int64_t val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, max((long long)val, (long long)assumed));
  } while (assumed != old);

  return old;
}

__device__ int64_t atomicMin64(int64_t* address, int64_t val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, min((long long)val, (long long)assumed));
  } while (assumed != old);

  return old;
}

// As of 20160418, CUDA 8.0EA only defines `atomicAdd(double*, double)` for compute capability >= 6.0.
#if CUDA_VERSION < 8000 || (defined(__CUDA_ARCH__) && __CUDA_ARCH__ < 600)
__device__ double atomicAdd(double* address, double val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, __double_as_longlong(val + __longlong_as_double(assumed)));

    // Note: uses integer comparison to avoid hang in case of NaN (since NaN != NaN)
  } while (assumed != old);

  return __longlong_as_double(old);
}
#endif

__device__ double atomicMax(double* address, double val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, __double_as_longlong(max(val, __longlong_as_double(assumed))));

    // Note: uses integer comparison to avoid hang in case of NaN (since NaN != NaN)
  } while (assumed != old);

  return __longlong_as_double(old);
}

__device__ float atomicMax(float* address, float val) {
  int* address_as_int = (int*)address;
  int old = *address_as_int, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_int, assumed, __float_as_int(max(val, __int_as_float(assumed))));

    // Note: uses integer comparison to avoid hang in case of NaN (since NaN != NaN)
  } while (assumed != old);

  return __int_as_float(old);
}

__device__ double atomicMin(double* address, double val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, __double_as_longlong(min(val, __longlong_as_double(assumed))));
  } while (assumed != old);

  return __longlong_as_double(old);
}

__device__ double atomicMin(float* address, float val) {
  int* address_as_ull = (int*)address;
  int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, __float_as_int(min(val, __int_as_float(assumed))));
  } while (assumed != old);

  return __int_as_float(old);
}

extern "C" __device__ uint64_t agg_count_shared(uint64_t* agg, const int64_t val) {
  return static_cast<uint64_t>(atomicAdd(reinterpret_cast<uint32_t*>(agg), 1UL));
}

extern "C" __device__ uint32_t agg_count_int32_shared(uint32_t* agg, const int32_t val) {
  return atomicAdd(agg, 1UL);
}

extern "C" __device__ uint64_t agg_count_double_shared(uint64_t* agg, const double val) {
  return agg_count_shared(agg, val);
}

extern "C" __device__ uint32_t agg_count_float_shared(uint32_t* agg, const float val) {
  return agg_count_int32_shared(agg, val);
}

extern "C" __device__ int64_t agg_sum_shared(int64_t* agg, const int64_t val) {
  return atomicAdd(reinterpret_cast<unsigned long long*>(agg), val);
}

extern "C" __device__ int32_t agg_sum_int32_shared(int32_t* agg, const int32_t val) {
  return atomicAdd(agg, val);
}

extern "C" __device__ void agg_sum_float_shared(int32_t* agg, const float val) {
  atomicAdd(reinterpret_cast<float*>(agg), val);
}

extern "C" __device__ void agg_sum_double_shared(int64_t* agg, const double val) {
  atomicAdd(reinterpret_cast<double*>(agg), val);
}

extern "C" __device__ void agg_max_shared(int64_t* agg, const int64_t val) {
  atomicMax64(agg, val);
}

extern "C" __device__ void agg_max_int32_shared(int32_t* agg, const int32_t val) {
  atomicMax(agg, val);
}

extern "C" __device__ void agg_max_double_shared(int64_t* agg, const double val) {
  atomicMax(reinterpret_cast<double*>(agg), val);
}

extern "C" __device__ void agg_max_float_shared(int32_t* agg, const float val) {
  atomicMax(reinterpret_cast<float*>(agg), val);
}

extern "C" __device__ void agg_min_shared(int64_t* agg, const int64_t val) {
  atomicMin64(agg, val);
}

extern "C" __device__ void agg_min_int32_shared(int32_t* agg, const int32_t val) {
  atomicMin(agg, val);
}

extern "C" __device__ void agg_min_double_shared(int64_t* agg, const double val) {
  atomicMin(reinterpret_cast<double*>(agg), val);
}

extern "C" __device__ void agg_min_float_shared(int32_t* agg, const float val) {
  atomicMin(reinterpret_cast<float*>(agg), val);
}

extern "C" __device__ void agg_id_shared(int64_t* agg, const int64_t val) {
  *agg = val;
}

extern "C" __device__ void agg_id_int32_shared(int32_t* agg, const int32_t val) {
  *agg = val;
}

extern "C" __device__ void agg_id_double_shared(int64_t* agg, const double val) {
  *agg = *(reinterpret_cast<const int64_t*>(&val));
}

extern "C" __device__ void agg_id_double_shared_slow(int64_t* agg, const double* val) {
  *agg = *(reinterpret_cast<const int64_t*>(val));
}

extern "C" __device__ void agg_id_float_shared(int32_t* agg, const float val) {
  *agg = __float_as_int(val);
}

#define DEF_SKIP_AGG(base_agg_func)                                                                                    \
  extern "C" __device__ ADDR_T base_agg_func##_skip_val_shared(ADDR_T* agg, const DATA_T val, const DATA_T skip_val) { \
    if (val != skip_val) {                                                                                             \
      return base_agg_func##_shared(agg, val);                                                                         \
    }                                                                                                                  \
    return 0;                                                                                                          \
  }

#define DATA_T int64_t
#define ADDR_T uint64_t
DEF_SKIP_AGG(agg_count)
#undef DATA_T
#undef ADDR_T

#define DATA_T int32_t
#define ADDR_T uint32_t
DEF_SKIP_AGG(agg_count_int32)
#undef DATA_T
#undef ADDR_T

// Initial value for nullable column is INT32_MIN
extern "C" __device__ void agg_max_int32_skip_val_shared(int32_t* agg, const int32_t val, const int32_t skip_val) {
  if (val != skip_val) {
    agg_max_int32_shared(agg, val);
  }
}

__device__ int32_t atomicMin32SkipVal(int32_t* address, int32_t val, const int32_t skip_val) {
  int32_t old = atomicExch(address, INT_MAX);
  return atomicMin(address, old == skip_val ? val : min(old, val));
}

extern "C" __device__ void agg_min_int32_skip_val_shared(int32_t* agg, const int32_t val, const int32_t skip_val) {
  if (val != skip_val) {
    atomicMin32SkipVal(agg, val, skip_val);
  }
}

__device__ int32_t atomicSum32SkipVal(int32_t* address, const int32_t val, const int32_t skip_val) {
  unsigned int* address_as_int = (unsigned int*)address;
  int32_t old = atomicExch(address_as_int, 0);
  int32_t old2 = atomicAdd(address_as_int, old == skip_val ? val : (val + old));
  return old == skip_val ? old2 : (old2 + old);
}

extern "C" __device__ int32_t agg_sum_int32_skip_val_shared(int32_t* agg, const int32_t val, const int32_t skip_val) {
  if (val != skip_val) {
    const int32_t old = atomicSum32SkipVal(agg, val, skip_val);
    return old;
  }
  return 0;
}

__device__ int64_t atomicSum64SkipVal(int64_t* address, const int64_t val, const int64_t skip_val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  int64_t old = atomicExch(address_as_ull, 0);
  int64_t old2 = atomicAdd(address_as_ull, old == skip_val ? val : (val + old));
  return old == skip_val ? old2 : (old2 + old);
}

extern "C" __device__ int64_t agg_sum_skip_val_shared(int64_t* agg, const int64_t val, const int64_t skip_val) {
  if (val != skip_val) {
    return atomicSum64SkipVal(agg, val, skip_val);
  }
  return 0;
}

__device__ int64_t atomicMin64SkipVal(int64_t* address, int64_t val, const int64_t skip_val) {
  unsigned long long int* address_as_ull = reinterpret_cast<unsigned long long int*>(address);
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, assumed == skip_val ? val : min((long long)val, (long long)assumed));
  } while (assumed != old);

  return old;
}

extern "C" __device__ void agg_min_skip_val_shared(int64_t* agg, const int64_t val, const int64_t skip_val) {
  if (val != skip_val) {
    atomicMin64SkipVal(agg, val, skip_val);
  }
}

__device__ int64_t atomicMax64SkipVal(int64_t* address, int64_t val, const int64_t skip_val) {
  unsigned long long int* address_as_ull = reinterpret_cast<unsigned long long int*>(address);
  unsigned long long int old = *address_as_ull, assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull, assumed, assumed == skip_val ? val : max((long long)val, (long long)assumed));
  } while (assumed != old);

  return old;
}

extern "C" __device__ void agg_max_skip_val_shared(int64_t* agg, const int64_t val, const int64_t skip_val) {
  if (val != skip_val) {
    atomicMax64SkipVal(agg, val, skip_val);
  }
}

#undef DEF_SKIP_AGG
#define DEF_SKIP_AGG(base_agg_func)                                                                                    \
  extern "C" __device__ ADDR_T base_agg_func##_skip_val_shared(ADDR_T* agg, const DATA_T val, const DATA_T skip_val) { \
    if (val != skip_val) {                                                                                             \
      return base_agg_func##_shared(agg, val);                                                                         \
    }                                                                                                                  \
    return *agg;                                                                                                       \
  }

#define DATA_T double
#define ADDR_T uint64_t
DEF_SKIP_AGG(agg_count_double)
#undef ADDR_T
#undef DATA_T

#define DATA_T float
#define ADDR_T uint32_t
DEF_SKIP_AGG(agg_count_float)
#undef ADDR_T
#undef DATA_T

// Initial value for nullable column is FLOAT_MIN
extern "C" __device__ void agg_max_float_skip_val_shared(int32_t* agg, const float val, const float skip_val) {
  if (__float_as_int(val) != __float_as_int(skip_val)) {
    float old = atomicExch(reinterpret_cast<float*>(agg), -FLT_MAX);
    atomicMax(reinterpret_cast<float*>(agg), __float_as_int(old) == __float_as_int(skip_val) ? val : fmaxf(old, val));
  }
}

__device__ float atomicMinFltSkipVal(int32_t* address, float val, const float skip_val) {
  float old = atomicExch(reinterpret_cast<float*>(address), FLT_MAX);
  return atomicMin(reinterpret_cast<float*>(address),
                   __float_as_int(old) == __float_as_int(skip_val) ? val : fminf(old, val));
}

extern "C" __device__ void agg_min_float_skip_val_shared(int32_t* agg, const float val, const float skip_val) {
  if (__float_as_int(val) != __float_as_int(skip_val)) {
    atomicMinFltSkipVal(agg, val, skip_val);
  }
}

__device__ void atomicSumFltSkipVal(float* address, const float val, const float skip_val) {
  float old = atomicExch(address, 0.f);
  atomicAdd(address, __float_as_int(old) == __float_as_int(skip_val) ? val : (val + old));
}

extern "C" __device__ void agg_sum_float_skip_val_shared(int32_t* agg, const float val, const float skip_val) {
  if (__float_as_int(val) != __float_as_int(skip_val)) {
    atomicSumFltSkipVal(reinterpret_cast<float*>(agg), val, skip_val);
  }
}

__device__ void atomicSumDblSkipVal(double* address, const double val, const double skip_val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  double old = __longlong_as_double(atomicExch(address_as_ull, __double_as_longlong(0.)));
  atomicAdd(address, __double_as_longlong(old) == __double_as_longlong(skip_val) ? val : (val + old));
}

extern "C" __device__ void agg_sum_double_skip_val_shared(int64_t* agg, const double val, const double skip_val) {
  if (__double_as_longlong(val) != __double_as_longlong(skip_val)) {
    atomicSumDblSkipVal(reinterpret_cast<double*>(agg), val, skip_val);
  }
}

__device__ double atomicMinDblSkipVal(double* address, double val, const double skip_val) {
  unsigned long long int* address_as_ull = reinterpret_cast<unsigned long long int*>(address);
  unsigned long long int old = *address_as_ull;
  unsigned long long int skip_val_as_ull = *reinterpret_cast<const unsigned long long*>(&skip_val);
  unsigned long long int assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull,
                    assumed,
                    assumed == skip_val_as_ull ? *reinterpret_cast<unsigned long long*>(&val)
                                               : __double_as_longlong(min(val, __longlong_as_double(assumed))));
  } while (assumed != old);

  return __longlong_as_double(old);
}

extern "C" __device__ void agg_min_double_skip_val_shared(int64_t* agg, const double val, const double skip_val) {
  if (val != skip_val) {
    atomicMinDblSkipVal(reinterpret_cast<double*>(agg), val, skip_val);
  }
}

__device__ double atomicMaxDblSkipVal(double* address, double val, const double skip_val) {
  unsigned long long int* address_as_ull = (unsigned long long int*)address;
  unsigned long long int old = *address_as_ull;
  unsigned long long int skip_val_as_ull = *((unsigned long long int*)&skip_val);
  unsigned long long int assumed;

  do {
    assumed = old;
    old = atomicCAS(address_as_ull,
                    assumed,
                    assumed == skip_val_as_ull ? *((unsigned long long int*)&val)
                                               : __double_as_longlong(max(val, __longlong_as_double(assumed))));
  } while (assumed != old);

  return __longlong_as_double(old);
}

extern "C" __device__ void agg_max_double_skip_val_shared(int64_t* agg, const double val, const double skip_val) {
  if (val != skip_val) {
    atomicMaxDblSkipVal(reinterpret_cast<double*>(agg), val, skip_val);
  }
}

#undef DEF_SKIP_AGG

#include "ExtractFromTime.cpp"
#include "DateTruncate.cpp"
#include "../Utils/ChunkIter.cpp"
#define EXECUTE_INCLUDE
#include "DateAdd.cpp"
#include "ArrayOps.cpp"
#include "StringFunctions.cpp"
#undef EXECUTE_INCLUDE
#include "../Utils/StringLike.cpp"
#include "../Utils/Regexp.cpp"

extern "C" __device__ uint64_t string_decode(int8_t* chunk_iter_, int64_t pos) {
  // TODO(alex): de-dup, the x64 version is basically identical
  ChunkIter* chunk_iter = reinterpret_cast<ChunkIter*>(chunk_iter_);
  VarlenDatum vd;
  bool is_end;
  ChunkIter_get_nth(chunk_iter, pos, false, &vd, &is_end);
  return vd.is_null ? 0 : (reinterpret_cast<uint64_t>(vd.pointer) & 0xffffffffffff) |
                              (static_cast<uint64_t>(vd.length) << 48);
}

extern "C" __device__ void linear_probabilistic_count(uint8_t* bitmap,
                                                      const uint32_t bitmap_bytes,
                                                      const uint8_t* key_bytes,
                                                      const uint32_t key_len) {
  const uint32_t bit_pos = MurmurHash1(key_bytes, key_len, 0) % (bitmap_bytes * 8);
  const uint32_t word_idx = bit_pos / 32;
  const uint32_t bit_idx = bit_pos % 32;
  atomicOr(((uint32_t*)bitmap) + word_idx, 1 << bit_idx);
}

extern "C" __device__ void agg_count_distinct_bitmap_gpu(int64_t* agg,
                                                         const int64_t val,
                                                         const int64_t min_val,
                                                         const int64_t base_dev_addr,
                                                         const int64_t base_host_addr,
                                                         const uint64_t sub_bitmap_count,
                                                         const uint64_t bitmap_bytes) {
  const uint64_t bitmap_idx = val - min_val;
  const uint32_t byte_idx = bitmap_idx >> 3;
  const uint32_t word_idx = byte_idx >> 2;
  const uint32_t byte_word_idx = byte_idx & 3;
  const int64_t host_addr = *agg;
  uint32_t* bitmap =
      (uint32_t*)(base_dev_addr + host_addr - base_host_addr + (threadIdx.x & (sub_bitmap_count - 1)) * bitmap_bytes);
  switch (byte_word_idx) {
    case 0:
      atomicOr(&bitmap[word_idx], 1 << (bitmap_idx & 7));
      break;
    case 1:
      atomicOr(&bitmap[word_idx], 1 << ((bitmap_idx & 7) + 8));
      break;
    case 2:
      atomicOr(&bitmap[word_idx], 1 << ((bitmap_idx & 7) + 16));
      break;
    case 3:
      atomicOr(&bitmap[word_idx], 1 << ((bitmap_idx & 7) + 24));
      break;
    default:
      break;
  }
}

extern "C" __device__ void agg_count_distinct_bitmap_skip_val_gpu(int64_t* agg,
                                                                  const int64_t val,
                                                                  const int64_t min_val,
                                                                  const int64_t skip_val,
                                                                  const int64_t base_dev_addr,
                                                                  const int64_t base_host_addr,
                                                                  const uint64_t sub_bitmap_count,
                                                                  const uint64_t bitmap_bytes) {
  if (val != skip_val) {
    agg_count_distinct_bitmap_gpu(agg, val, min_val, base_dev_addr, base_host_addr, sub_bitmap_count, bitmap_bytes);
  }
}

extern "C" __device__ void agg_approximate_count_distinct_gpu(int64_t* agg,
                                                              const int64_t key,
                                                              const uint32_t b,
                                                              const int64_t base_dev_addr,
                                                              const int64_t base_host_addr) {
  const uint64_t hash = MurmurHash64A(&key, sizeof(key), 0);
  const uint32_t index = hash >> (64 - b);
  const int32_t rank = get_rank(hash << b, 64 - b);
  const int64_t host_addr = *agg;
  int32_t* M = (int32_t*)(base_dev_addr + host_addr - base_host_addr);
  atomicMax(&M[index], rank);
}

#define RANDOM_GEN_SCHEME_MOD 2147483647
#define RANDOM_GEN_SCHEME_HL 31

/*
  Adapted from MassDal: http://www.cs.rutgers.edu/~muthu/massdal-code-index.html
  Computes Carter-Wegman (CW) hash with Mersenne trick
*/
extern "C" __device__ uint32_t hash31(uint64_t a, uint64_t b, uint64_t x) {
  uint64_t result;
  uint32_t lresult;

  result = (a * x) + b;
  result = ((result >> (uint32_t)RANDOM_GEN_SCHEME_HL) + result) & (uint64_t)RANDOM_GEN_SCHEME_MOD;
  lresult = (uint32_t)result;

  return lresult;
}

/*
  b-valued random variables
  2-wise and 4-wise CW scheme
*/
extern "C" __device__ uint32_t CW2B(uint64_t a, uint64_t b, uint64_t x, uint32_t M) {
  uint32_t p_res = hash31(a, b, x);
  if (M == RANDOM_GEN_SCHEME_MOD)
    return p_res;

  uint32_t res = p_res % M;
  return res;
}

/*
  Computes parity bit of the bits of an integer
*/
extern "C" __device__ uint32_t seq_xor(uint32_t x) {
  x ^= (x >> 16);
  x ^= (x >> 8);
  x ^= (x >> 4);
  x ^= (x >> 2);
  x ^= (x >> 1);

  return (x & 1U);
}

extern "C" __device__ int32_t EH3(uint32_t i0, uint32_t i1, uint32_t j) {
  uint32_t mask = 0xAAAAAAAA;
  uint32_t p_res = (i1 & j) ^ (j & (j << 1) & mask);

  int32_t res = ((i0 ^ seq_xor(p_res)) & (1U == 1U)) ? 1 : -1;
  return res;
}

// sketch per block approach (no synchronization)
extern "C" __device__ void agg_count_and_update_sketch_gpu2(int64_t* counter,
                                                           int64_t* sketch_data,
                                                            const uint8_t num_cols_to_update,
                                                            const int64_t* keys,
                                                            const uint32_t* col_indices,
                                                            const uint32_t* xi_bucket,
                                                            const uint32_t* xi_pm1,
                                                            const uint16_t num_buckets,
                                                            const uint16_t num_rows,
                                                            const int64_t base_dev_addr,
                                                            const int64_t base_host_addr) {

  uint64_t sketch_size = num_buckets * num_rows; // one element for counter
  uint64_t block_set = (sketch_size + blockDim.x) * blockIdx.x; // shared memory for block

  uint64_t sketch_start_point = block_set + blockDim.x;

  sketch_data[block_set + threadIdx.x] += 1;

  for (uint16_t i = 0; i < num_rows; i++) {
    for (uint16_t j = 0; j < num_cols_to_update; j++) {

      uint32_t first_seed_idx = 2 * (num_cols_to_update * i + j);
      uint32_t second_seed_idx = first_seed_idx + 1;

      int32_t b = CW2B(xi_bucket[first_seed_idx], xi_bucket[second_seed_idx], keys[j], num_buckets);

      uint32_t bucket_idx = i * num_buckets + b;

      sketch_data[sketch_start_point + bucket_idx] += EH3(xi_pm1[first_seed_idx], xi_pm1[second_seed_idx], keys[j]) * 1.0;
    }
  }
}

// separate sketch per col approach, per block approach (no synchronization)
extern "C" __device__ void agg_count_and_update_sketch_gpu(int64_t* counter,
                                                            int64_t* sketch_data,
                                                            const uint8_t num_cols_to_update,
                                                            const int64_t* keys,
                                                            const uint32_t* col_indices,
                                                            const uint32_t* xi_bucket,
                                                            const uint32_t* xi_pm1,
                                                            const uint16_t num_buckets,
                                                            const uint16_t num_rows,
                                                            const int64_t base_dev_addr,
                                                            const int64_t base_host_addr) {

  uint64_t sketch_size = num_buckets * num_rows; // one element for counter
  uint64_t block_set = (sketch_size * num_cols_to_update + blockDim.x) * blockIdx.x; // shared memory for block

  uint64_t sketch_start_point = block_set + blockDim.x; // sketch data starts after the block counters

  sketch_data[block_set + threadIdx.x] += 1;
  uint32_t row_first_modulo = threadIdx.x % num_rows;

  for (uint16_t i = 0; i < num_rows; i++) {

    uint32_t row_idx = (row_first_modulo + i) % num_rows;  // shuffle the updates within wrap (32 SIMD threads)

    uint32_t row_buckets = row_idx * num_buckets; // bucket set of row i

    for (uint16_t j = 0; j < num_cols_to_update; j++) {

      uint32_t first_seed_idx = 2 * (num_cols_to_update * row_idx + j);
      uint32_t second_seed_idx = first_seed_idx + 1;

      int32_t b = CW2B(xi_bucket[first_seed_idx], xi_bucket[second_seed_idx], keys[j], num_buckets);

      uint32_t bucket_idx = j * sketch_size + row_buckets + b;

      sketch_data[sketch_start_point + bucket_idx] += EH3(xi_pm1[first_seed_idx], xi_pm1[second_seed_idx], keys[j]) * 1.0;
    }
  }
}

// sketch per thread approach
extern "C" __device__ void agg_count_and_update_sketch_gpu1(int64_t* counter,
                                                           int64_t* sketch_data,
                                                           const uint8_t num_cols_to_update,
                                                           const int64_t* keys,
                                                           const uint32_t* col_indices,
                                                           const uint32_t* xi_bucket,
                                                           const uint32_t* xi_pm1,
                                                           const uint16_t num_buckets,
                                                           const uint16_t num_rows,
                                                           const int64_t base_dev_addr,
                                                           const int64_t base_host_addr) {

  uint64_t sketch_size = num_buckets * num_rows + 1;
  uint64_t sketch_set = sketch_size * threadIdx.x;
  uint64_t block_set = blockDim.x * sketch_size * blockIdx.x;
  sketch_data[block_set + sketch_set] += 1;

  uint64_t sketch_start_point = block_set + sketch_set;

  for (uint16_t i = 0; i < num_rows; i++) {
    for (uint16_t j = 0; j < num_cols_to_update; j++) {
      uint32_t first_seed_idx = 2 * (num_cols_to_update * i + j);
      uint32_t second_seed_idx = first_seed_idx + 1;

      int32_t b = CW2B(xi_bucket[first_seed_idx], xi_bucket[second_seed_idx], keys[j], num_buckets);
      uint32_t bucket_idx = 1 + i * num_buckets + b;
      sketch_data[sketch_start_point + bucket_idx] += EH3(xi_pm1[first_seed_idx], xi_pm1[second_seed_idx], keys[j]) * 1.0;
    }
  }
}

extern "C" __device__ void force_sync() {
  __threadfence_block();
}
