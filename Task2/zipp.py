"""Написать свою реализацию функции zip(), сшивающую произвольное количество итерируемых объектов
(по аналогии с встроенной в Python функцией) """

def my_zyp(*args) -> [list, str, dict]:
    """ Функция реализует объединение итерируемых объектов, имеющих отношение порядка, по индексу - списки, кортежи, а
    также осуществляет сшивку словарей по общим для всех словарей ключам """
    if len(args):
        # определяем типы данных для проверки на пригодность к объединению
        types = list(map(lambda x: str(type(x)), args))
        try:
            # все объекты - списки и/или кортежи - результатом будет список
            if all(map(lambda x: ('list' in x or 'tuple' in x), types)):
                result = []
                # получаем длину самого короткого объекта
                min_length = min(map(len, args))
                # проходим по индексам
                for index in range(min_length):
                    result.append(tuple(map(lambda x: x[index], args)))
                return result
            # все объекты - словари, результатом будет словарь
            elif all(map(lambda x: 'dict' in x, types)):
                # начальное значение - ключи самого длинного словаря
                shared_keys = set(max(args, key=lambda x: len(x)).keys())
                # обходим словари из списка аргументов
                for dict_ in args:
                     # пересекаем их ключи между собой
                    shared_keys = shared_keys.intersection(set(dict_.keys()))
                # результатом сшивки будет словарь
                result = {}
                for key in shared_keys:
                    result[key] = tuple(x.get(key) for x in args)
                return result
            else:
                return f'Данные не могут быть объединены, присутствуют объекты, не имеющие отношения порядка'
        except Exception() as e:
            return f'{e}'

    return f'Нет объектов для объединения в одну структуру'


print(my_zyp([1, 2, 3, 4], (5, 6, 7, 8), (1, 15, 25, 30)))
print(my_zyp({'1': 1, '2': 2, '3': 3}, {'1': 5, '3': 7, '4': 8, '5': 10}, {'3': 4, '2': 16, '1': 100, '4': 25}))
print(my_zyp([1, 2, 3], {1, 2, 3}, (1, 2, 3)))
print(my_zyp((1, 2), (3, 'a')))
print(my_zyp())