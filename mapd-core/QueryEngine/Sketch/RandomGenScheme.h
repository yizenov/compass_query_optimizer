#ifndef QUERYENGINE_RANDOMGENSCHEME_H
#define QUERYENGINE_RANDOMGENSCHEME_H

#include <cstdio>
#include <cstdlib>

#define RANDOMGENSCHEME_MOD 2147483647
#define RANDOMGENSCHEME_HL 31

/*
  Generating schemes for +-1 random variables
  For details see the papers:
        1) "Fast Range-Summable Random Variables for Efficient Aggregate Estimation"
        by F. Rusu and A. Dobra
        2) "Pseudo-Random Number Generation for Sketch-Based Estimations"
        by F. Rusu and A. Dobra

  Assumption: integers are represented on 32 bits
*/

inline unsigned int RandomGenerate() {
  unsigned int x = rand();
  unsigned int h = rand();

  return x ^ ((h & 1) << 31);
}

/*
  Computes parity bit of the bits of an integer
*/
inline unsigned int seq_xor(unsigned int x) {
  x ^= (x >> 16);
  x ^= (x >> 8);
  x ^= (x >> 4);
  x ^= (x >> 2);
  x ^= (x >> 1);

  return (x & 1U);
}

/*
  Adapted from MassDal: http://www.cs.rutgers.edu/~muthu/massdal-code-index.html
  Computes Carter-Wegman (CW) hash with Mersenne trick
*/
inline unsigned int hash31(unsigned long long a, unsigned long long b, unsigned long long x) {
  unsigned long long result;
  unsigned int lresult;

  result = (a * x) + b;
  result = ((result >> (unsigned int)RANDOMGENSCHEME_HL) + result) & (unsigned long long)RANDOMGENSCHEME_MOD;
  lresult = (unsigned int)result;

  return lresult;
}

/*
  +-1 random variables
  3-wise independent schemes
*/
inline int BCH3(unsigned int i0, unsigned int I1, unsigned int j) {
  int res = ((i0 ^ seq_xor(I1 & j)) & (1U == 1U)) ? 1 : -1;
  return res;
}

inline int EH3(unsigned int i0, unsigned int I1, unsigned int j) {
  unsigned int mask = 0xAAAAAAAA;
  unsigned int p_res = (I1 & j) ^ (j & (j << 1) & mask);

  int res = ((i0 ^ seq_xor(p_res)) & (1U == 1U)) ? 1 : -1;
  return res;
}

inline int CW2(unsigned long a, unsigned long b, unsigned long x) {
  unsigned int p_res = hash31(a, b, x);

  int res = (p_res & (1U == 1U)) ? 1 : -1;
  return res;
}

/*
  +-1 random variables
  5-wise independent schemes
*/
inline int BCH5(unsigned int i0, unsigned int I1, unsigned int I2, unsigned int j) {
  unsigned int p_res = (I1 & j) ^ (I2 & (j * j * j));

  int res = ((i0 ^ seq_xor(p_res)) & (1U == 1U)) ? 1 : -1;
  return res;
}

inline int RM7(unsigned int i0, unsigned int* I, unsigned int j) {
  unsigned int res = i0;

  int i, k;
  for (i = 0; i < 32; i++)
    res ^= (((I[i] & (1U << i)) >> i) & ((j & (1U << i)) >> i));

  for (i = 0; i < 32; i++) {
    for (k = i + 1; k < 32; k++) {
      unsigned int p_res = ((j & (1U << i)) >> i) & ((j & (1U << k)) >> k);
      p_res &= ((I[i] & (1U << k)) >> k);
      res ^= p_res;
    }
  }

  int ret = (res == 1U) ? 1 : -1;
  return ret;
}

inline int CW4(unsigned long a, unsigned long b, unsigned long c, unsigned long d, unsigned long x) {
  unsigned int p_res = hash31(hash31(hash31(a, b, x), c, x), d, x);

  int res = (p_res & (1U == 1U)) ? 1 : -1;
  return res;
}

/*
  b-valued random variables
  2-wise and 4-wise CW scheme
*/
inline unsigned int CW2B(unsigned long a, unsigned long b, unsigned long x, unsigned int M) {
  unsigned int p_res = hash31(a, b, x);
  if (M == RANDOMGENSCHEME_MOD)
    return p_res;

  unsigned int res = p_res % M;
  return res;
}

inline unsigned int CW4B(unsigned long a,
                         unsigned long b,
                         unsigned long c,
                         unsigned long d,
                         unsigned long x,
                         unsigned int M) {
  unsigned int p_res = hash31(hash31(hash31(a, b, x), c, x), d, x);
  if (M == RANDOMGENSCHEME_MOD)
    return p_res;

  unsigned int res = p_res % M;
  return res;
}

#endif  // QUERYENGINE_RANDOMGENSCHEME_H
