#include "SketchUtil.h"

double Average(double* _x, int _n) {
  double sum = 0;
  for (int i = 0; i < _n; i++)
    sum += _x[i];

  sum = sum / (double)_n;
  return sum;
}

double Median(double* _x, int _n) {
  if (_n == 1)
    return _x[0];
  if (_n == 2)
    return (_x[0] + _x[1]) / 2;

  double* X = new double[_n];
  for (int i = 0; i < _n; i++)
    X[i] = _x[i];

  // implement bubble sort
  bool rpt = true;
  while (rpt) {
    rpt = false;

    for (int i = 0; i < _n - 1; i++)
      if (X[i] > X[i + 1]) {
        double t = X[i];
        X[i] = X[i + 1];
        X[i + 1] = t;
        rpt = true;
      }
  }

  double res;
  if (_n % 2 == 0)
    res = (X[_n / 2 - 1] + X[_n / 2]) / 2.0;
  else
    res = X[_n / 2];

  delete[] X;

  return res;
}

double Min(double* _x, int _n) {
  if (_n == 1)
    return _x[0];
  if (_n == 2)
    return (_x[0] <= _x[1] ? _x[0] : _x[1]);

  double min = _x[0];
  for (int i = 1; i < _n; i++)
    if (_x[i] < min)
      min = _x[i];

  return min;
}