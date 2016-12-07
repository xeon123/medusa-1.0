def test_dict(d, ref):
    # update dict if the key exists
    if ref not in d.values():
        # add new element to the dict
        d.update({('y', 'z'): ref})
    else:
        for idx, k in enumerate(d):
            if ref == d[k]:
                values = d[k]
                del (d[k])
                k_list = list(k)
                k_list.append('e')
                k = tuple(k_list)

                # replace the entry in the d
                d.update({k: values})

    print d


if __name__ == "__main__":
    d = {('a', 'b'): [1, 2, 3], ('c', 'd'): [4, 5, 6]}
    test_dict(d, [1, 2, 3])
    test_dict(d, [7, 8, 9])
