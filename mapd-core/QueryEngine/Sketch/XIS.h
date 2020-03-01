#ifndef QUERYENGINE_XIS_H
#define QUERYENGINE_XIS_H

#include <cstring>
#include <fstream>
#include <iostream>

// #include "RandomGenScheme.h"
#include "RandomRangeSum.h"

/*
  Generating schemes for different types of random variables
    -> +/-1 random variables
    -> k-valued random variables
  Fast range-summable random variables
  For details see the papers:
        1) "Fast Range-Summable Random Variables for Efficient Aggregate Estimation"
                by F. Rusu and A. Dobra
        2) "Pseudo-Random Number Generation for Sketch-Based Estimations" by F. Rusu
                and A. Dobra
*/

class Xi {
 public:
  Xi() {}
  virtual ~Xi() {}

  virtual double element(unsigned int j) = 0;
  virtual double interval_sum(unsigned int alpha, unsigned int beta) = 0;
};

/*
+/-1 random variables that are 3-wise independent
*/

class Xi_BCH3 : public Xi {
 public:
  unsigned int seeds[2];

  Xi_BCH3() {}

  Xi_BCH3(unsigned int I1, unsigned int I2) {
    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_BCH3() {}

  Xi_BCH3& operator=(const Xi_BCH3& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];

    double res = BCH3(i0, i1, j);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];

    double res = BCH3_Range(alpha, beta, i1, i0);
    return res;
  }
};

class Xi_EH3 : public Xi {
 public:
  unsigned int seeds[2];

  Xi_EH3() {
    seeds[0] = 0;
    seeds[1] = 0;
  }

  Xi_EH3(unsigned int I1, unsigned int I2) {
    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_EH3() {}

  Xi_EH3& operator=(const Xi_EH3& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];

    double res = EH3(i0, i1, j);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];

    double res = EH3_Range(alpha, beta, i1, i0);
    return res;
  }

  // overload print operator
  friend std::ostream& operator<<(std::ostream& _os, Xi_EH3& _xi);
};

class Xi_CW2 : public Xi {
 public:
  unsigned int seeds[2];

  Xi_CW2() {}

  Xi_CW2(unsigned int I1, unsigned int I2) {
    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_CW2() {}

  Xi_CW2& operator=(const Xi_CW2& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned long a = seeds[0];
    unsigned long b = seeds[1];

    double res = CW2(a, b, j);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) {
    unsigned long a = seeds[0];
    unsigned long b = seeds[1];

    double res = 0;

    for (unsigned int k = alpha; k <= beta; k++)
      res += CW2(a, b, k);

    return res;
  }
};

/*
B-valued random variables that are 2-wise independent
*/

class Xi_CW2B : public Xi {
 public:
  unsigned int seeds[2];
  unsigned int buckets_no;

  Xi_CW2B() : buckets_no(RANDOMGENSCHEME_MOD) {
    seeds[0] = 0;
    seeds[1] = 0;
  }

  Xi_CW2B(unsigned int I1, unsigned int I2, unsigned int B = RANDOMGENSCHEME_MOD) {
    buckets_no = B;

    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_CW2B() {}

  Xi_CW2B& operator=(const Xi_CW2B& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    buckets_no = _me.buckets_no;
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned long a = seeds[0];
    unsigned long b = seeds[1];

    double res = CW2B(a, b, j, buckets_no);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) { return -1; }

  // overload print operator
  friend std::ostream& operator<<(std::ostream& _os, Xi_CW2B& _xi);
};

/*
+/-1 random variables that are 4-wise independent
*/

class Xi_CW4 : public Xi {
 public:
  unsigned int seeds[4];

  Xi_CW4() {}

  Xi_CW4(unsigned int I1, unsigned int I2) {
    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[2] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[3] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_CW4() {}

  Xi_CW4& operator=(const Xi_CW4& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned long a = seeds[0];
    unsigned long b = seeds[1];
    unsigned long c = seeds[2];
    unsigned long d = seeds[3];

    double res = CW4(a, b, c, d, j);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) {
    unsigned long a = seeds[0];
    unsigned long b = seeds[1];
    unsigned long c = seeds[2];
    unsigned long d = seeds[3];

    double res = 0;

    for (unsigned int k = alpha; k <= beta; k++)
      res += CW4(a, b, c, d, k);

    return res;
  }
};

/*
B-valued random variables that are 4-wise independent
*/

class Xi_CW4B : public Xi {
 public:
  unsigned int seeds[4];
  unsigned int buckets_no;

  Xi_CW4B() : buckets_no(RANDOMGENSCHEME_MOD) {}

  Xi_CW4B(unsigned int I1, unsigned int I2, unsigned int B = RANDOMGENSCHEME_MOD) {
    buckets_no = B;

    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[2] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[3] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  Xi_CW4B& operator=(const Xi_CW4B& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    buckets_no = _me.buckets_no;
    return (*this);
  }

  virtual ~Xi_CW4B() {}

  virtual double element(unsigned int j) {
    unsigned long a = seeds[0];
    unsigned long b = seeds[1];
    unsigned long c = seeds[2];
    unsigned long d = seeds[3];

    double res = CW4B(a, b, c, d, j, buckets_no);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) { return -1; }
};

/*
+/-1 random variables that are 5-wise independent
*/

class Xi_BCH5 : public Xi {
 public:
  unsigned int seeds[3];

  Xi_BCH5() {}

  Xi_BCH5(unsigned int I1, unsigned int I2) {
    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[2] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_BCH5() {}

  Xi_BCH5& operator=(const Xi_BCH5& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];
    unsigned int i2 = seeds[2];

    double res = BCH5(i0, i1, i2, j);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];
    unsigned int i2 = seeds[2];

    double res = 0;

    for (unsigned int k = alpha; k <= beta; k++)
      res += BCH5(i0, i1, i2, k);

    return res;
  }
};

/*
Random variables for dyadic mapping as an alternative to fast range-summation
*/

class Xi_Dyadic_Map_EH3 : public Xi {
 public:
  unsigned int seeds[2];
  unsigned int dom_size;

  Xi_Dyadic_Map_EH3() : dom_size(0) {}

  Xi_Dyadic_Map_EH3(unsigned int Dom_size, unsigned int I1, unsigned int I2) {
    dom_size = Dom_size;

    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_Dyadic_Map_EH3() {}

  Xi_Dyadic_Map_EH3& operator=(const Xi_Dyadic_Map_EH3& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    dom_size = _me.dom_size;
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];

    unsigned int front_mask = 0;
    unsigned int map;

    double res = 0;

    for (unsigned k = 0; k < dom_size; k++) {
      map = front_mask ^ (j >> k);
      res += EH3(i0, i1, map);

      if ((j >> k) & (1U == 1U))
        j ^= (1 << k);

      front_mask = (front_mask >> 1) ^ (1U << dom_size);
    }

    res += EH3(i0, i1, front_mask);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];

    double res = Dyadic_Range_EH3(alpha, beta, i0, i1, dom_size);
    return res;
  }
};

class Xi_Dyadic_Map_BCH5 : public Xi {
 public:
  unsigned int seeds[3];
  unsigned int dom_size;

  Xi_Dyadic_Map_BCH5() : dom_size(0) {}

  Xi_Dyadic_Map_BCH5(unsigned int Dom_size, unsigned int I1, unsigned int I2) {
    dom_size = Dom_size;

    const unsigned int k_mask = 0xffffffff;

    seeds[0] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[1] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;

    I1 = 36969 * (I1 & 0177777) + (I1 >> 16);
    I2 = 18000 * (I2 & 0177777) + (I2 >> 16);

    seeds[2] = ((I1 << 16) ^ (I2 & 0177777)) & k_mask;
  }

  virtual ~Xi_Dyadic_Map_BCH5() {}

  Xi_Dyadic_Map_BCH5& operator=(const Xi_Dyadic_Map_BCH5& _me) {
    memcpy(seeds, _me.seeds, sizeof(seeds));
    dom_size = _me.dom_size;
    return (*this);
  }

  virtual double element(unsigned int j) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];
    unsigned int i2 = seeds[2];

    unsigned int front_mask = 0;
    unsigned int map;

    double res = 0;

    for (unsigned k = 0; k < dom_size; k++) {
      map = front_mask ^ (j >> k);
      res += BCH5(i0, i1, i2, map);

      if ((j >> k) & (1U == 1U))
        j ^= (1 << k);

      front_mask = (front_mask >> 1) ^ (1U << dom_size);
    }

    res += BCH5(i0, i1, i2, front_mask);
    return res;
  }

  virtual double interval_sum(unsigned int alpha, unsigned int beta) {
    unsigned int i0 = seeds[0];
    unsigned int i1 = seeds[1];
    unsigned int i2 = seeds[2];

    double res = Dyadic_Range_BCH5(alpha, beta, i0, i1, i2, dom_size);
    return res;
  }
};

inline std::ostream& operator<<(std::ostream& _os, Xi_BCH3& _xi) {
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_EH3& _xi) {
  _os << "{" << _xi.seeds[0] << ", " << _xi.seeds[1] << "}";
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_CW2& _xi) {
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_CW2B& _xi) {
  _os << "{B : " << _xi.buckets_no << " S : (" << _xi.seeds[0] << ", " << _xi.seeds[1] << ")}";
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_CW4& _xi) {
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_CW4B& _xi) {
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_BCH5& _xi) {
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_Dyadic_Map_EH3& _xi) {
  return _os;
}

inline std::ostream& operator<<(std::ostream& _os, Xi_Dyadic_Map_BCH5& _xi) {
  return _os;
}

#endif  // QUERYENGINE_XIS_H
