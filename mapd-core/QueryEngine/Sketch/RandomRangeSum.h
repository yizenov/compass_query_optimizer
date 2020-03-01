#ifndef QUERYENGINE_RANDOMRANGESUM_H
#define QUERYENGINE_RANDOMRANGESUM_H

#include "RandomGenScheme.h"

/*
  Fast-range summation of +-1 random variables
  DMAP avoidance of fast range-summation
  For details see the papers:
        1) "Fast Range-Summable Random Variables for Efficient Aggregate Estimation"
                by F. Rusu and A. Dobra
        2) "Pseudo-Random Number Generation for Sketch-Based Estimations" by F. Rusu
                and A. Dobra

  Assumption: integers are represented on 32 bits
*/

/*
  Fast range-summation algorithm for BCH3 scheme
  See the pseudocode in paper from point 2) above
*/

inline double BCH3Interval(unsigned int gamma, unsigned int S0, unsigned int s0) {
  int i;
  double sum = 0;

  if (gamma & (1U == 1U)) {
    gamma -= 1;
    sum += BCH3(s0, S0, gamma);
  }

  for (i = 1; i < 32; i++) {
    if (gamma == 0U)
      return sum;

    if ((S0 >> (i - 1)) & (1U == 1U))
      return sum;
    else  //(S0 >> (i-1)) & 1 == 0
    {
      if ((gamma >> i) & (1U == 1U)) {
        gamma ^= (1U << i);
        sum += BCH3(s0, S0, gamma) << i;
      }
    }
  }

  return sum;
}

inline double BCH3_Range(unsigned int alpha, unsigned int beta, unsigned int S0, unsigned int s0) {
  double sum = BCH3Interval(beta + 1, S0, s0);
  sum -= BCH3Interval(alpha, S0, s0);

  return sum;
}

/*
  Fast range-summation algorithm for EH3 scheme
  See the pseudocode in paper from point 2) above
*/

inline double EH3Interval(unsigned int gamma, unsigned int S0, unsigned int s0) {
  int i, s_bits_one = 0;
  double sum = 0;
  int f_A;
  unsigned int t_bits;
  unsigned int s_bits;

  s_bits = S0 & 3U;
  if (s_bits == 3U)
    s_bits_one = 1;

  t_bits = gamma & 3U;
  if (t_bits == 1U) {
    gamma -= 1;

    sum += EH3(s0, S0, gamma);
  } else if (t_bits == 2U) {
    gamma -= 2;

    sum += EH3(s0, S0, gamma);
    sum += EH3(s0, S0, gamma + 1);
  } else if (t_bits == 3U) {
    gamma -= 3;

    sum += EH3(s0, S0, gamma);
    sum += EH3(s0, S0, gamma + 1);
    sum += EH3(s0, S0, gamma + 2);
  }

  for (i = 2; i < 32; i += 2) {
    if (gamma == 0U)
      return sum;

    t_bits = (gamma >> i) & 3U;
    gamma ^= (t_bits << i);
    if (t_bits == 1U) {
      f_A = EH3(s0, S0, gamma);

      if (s_bits_one % 2 == 1U)
        sum -= (f_A << (i >> 1));
      else
        sum += (f_A << (i >> 1));
    } else if (t_bits == 2U) {
      f_A = EH3(s0, S0, gamma);

      if (s_bits_one % 2 == 1U)
        sum -= (f_A << (i >> 1));
      else
        sum += (f_A << (i >> 1));

      f_A = EH3(s0, S0, gamma + (1U << i));

      if (s_bits_one % 2 == 1U)
        sum -= (f_A << (i >> 1));
      else
        sum += (f_A << (i >> 1));
    } else if (t_bits == 3U) {
      f_A = EH3(s0, S0, gamma);

      if (s_bits_one % 2 == 1U)
        sum -= (f_A << (i >> 1));
      else
        sum += (f_A << (i >> 1));

      f_A = EH3(s0, S0, gamma + (1 << i));

      if (s_bits_one % 2 == 1U)
        sum -= (f_A << (i >> 1));
      else
        sum += (f_A << (i >> 1));

      f_A = EH3(s0, S0, gamma + (1U << (i + 1)));

      if (s_bits_one % 2 == 1U)
        sum -= (f_A << (i >> 1));
      else
        sum += (f_A << (i >> 1));
    }

    s_bits = (S0 >> i) & 3U;
    if (s_bits == 3U)
      s_bits_one += 1;
  }

  return sum;
}

inline double EH3_Range(unsigned int alpha, unsigned int beta, unsigned int S0, unsigned int s0) {
  double sum = EH3Interval(beta + 1, S0, s0);
  sum -= EH3Interval(alpha, S0, s0);

  return sum;
}

/*
  Dyadic mapping algorithms to avoid the need for fast range-summation
  Dyadic mapping applies to a larger number of schemes than fast range-summation
*/

inline double Dyadic_Range_EH3(unsigned int a, unsigned int b, unsigned int i0, unsigned int I1, int dom_size) {
  double sum = 0;

  unsigned int pw = 0;
  unsigned int front_mask = 0;

  unsigned int map;

  int alpha = a;
  int beta = b;

  while (alpha <= beta) {
    if (((alpha >> pw) & 1U) == 1U) {
      map = front_mask ^ (alpha >> pw);
      sum += EH3(i0, I1, map);

      alpha += (1 << pw);
    }

    if (alpha > beta)
      return sum;

    if (((beta >> pw) & 1U) == 0U) {
      map = front_mask ^ ((beta - (1 << pw) + 1) >> pw);
      sum += EH3(i0, I1, map);

      beta -= (1 << pw);
    }

    front_mask = (front_mask >> 1) ^ (1U << dom_size);
    pw += 1;
  }

  return sum;
}

inline double Dyadic_Range_BCH5(unsigned int a,
                                unsigned int b,
                                unsigned int i0,
                                unsigned int I1,
                                unsigned int I2,
                                int dom_size) {
  double sum = 0;

  unsigned int pw = 0;
  unsigned int front_mask = 0;

  unsigned int map;

  int alpha = a;
  int beta = b;

  while (alpha <= beta) {
    if (((alpha >> pw) & 1U) == 1U) {
      map = front_mask ^ (alpha >> pw);
      sum += BCH5(i0, I1, I2, map);

      alpha += (1 << pw);
    }

    if (alpha > beta)
      return sum;

    if (((beta >> pw) & 1U) == 0U) {
      map = front_mask ^ ((beta - (1 << pw) + 1) >> pw);
      sum += BCH5(i0, I1, I2, map);

      beta -= (1 << pw);
    }

    front_mask = (front_mask >> 1) ^ (1U << dom_size);
    pw += 1;
  }

  return sum;
}

inline double Dyadic_Range_CW4(unsigned int aa,
                               unsigned int bb,
                               unsigned long a,
                               unsigned long b,
                               unsigned long c,
                               unsigned long d,
                               int dom_size) {
  double sum = 0;

  unsigned int pw = 0;
  unsigned int front_mask = 0;

  unsigned int map;

  int alpha = aa;
  int beta = bb;

  while (alpha <= beta) {
    if (((alpha >> pw) & 1U) == 1U) {
      map = front_mask ^ (alpha >> pw);
      sum += CW4(a, b, c, d, map);

      alpha += (1 << pw);
    }

    if (alpha > beta)
      return sum;

    if (((beta >> pw) & 1U) == 0U) {
      map = front_mask ^ ((beta - (1 << pw) + 1) >> pw);
      sum += CW4(a, b, c, d, map);

      beta -= (1 << pw);
    }

    front_mask = (front_mask >> 1) ^ (1U << dom_size);
    pw += 1;
  }

  return sum;
}

#endif  // QUERYENGINE_RANDOMRANGESUM_H
