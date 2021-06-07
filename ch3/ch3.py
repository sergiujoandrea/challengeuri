import multiprocessing


def gaseste_suma(numar):
    suma = sum(i * i for i in range(numar))
    t = (numar, suma)
    return t


if __name__ == '__main__':
    numere = [10, 20, 30, 40]
    p = multiprocessing.Pool()
    d = dict(p.map(gaseste_suma, numere))
    print(d)
