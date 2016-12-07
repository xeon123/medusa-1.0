

def test_concatenation():
    a = [(
        '/users/xeon/Programs/hadoop/bin/hadoop jar /users/xeon/Programs/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.5-alpha.jar wordcount /wiki /wiki-output', [['9493985885f1acd67f91eb1c725fe4c30a6d46aff62b1e80d42dfb490bb84d4d'], ['9493985885f1acd67f91eb1c725fe4c30a6d46aff62b1e80d42dfb490bb84d4d']]), ('/users/xeon/Programs/hadoop/bin/hadoop jar /users/xeon/Programs/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.5-alpha.jar wordcount /wiki2 /wiki-output2',
                                                                                                                                                                                                                                                                                                                                [['84e3b5fd22241b9059d673dce2c63810ec035e31cef57c25ef53cdd98f99f9f6'], ['84e3b5fd22241b9059d673dce2c63810ec035e31cef57c25ef53cdd98f99f9f6']]), ('/users/xeon/Programs/hadoop/bin/hadoop jar /users/xeon/Programs/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.5-alpha.jar wordcount /wiki3 /wiki-output3', [['c24c045a1e1f6a5980f44ff87d7e62dfdf4666a8e0ea197fa783508e6cd4194a'], ['c24c045a1e1f6a5980f44ff87d7e62dfdf4666a8e0ea197fa783508e6cd4194a']])]
    b = [(
        '/users/xeon/Programs/hadoop/bin/hadoop jar /users/xeon/Programs/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.5-alpha.jar wordcount /wiki2 /wiki-output2',
        [['84e3b5fd22241b9059d673dce2c63810ec035e31cef57c25ef53cdd98f99f9f6']])]

    for idx, x in enumerate(a):
        if b[0][0] == x[0]:
            concat_matrix = x[1] + b[0][1]
            l = list(x)
            l[1] = concat_matrix
            a[idx] = tuple(l)

    print a

if __name__ == "__main__":
    test_concatenation()
