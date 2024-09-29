def reduce(my_index, input):
    print(f'Reducer Index: {my_index} Path: {input}')
    file = f'{input}/input.txt'
    all_kv_pairs = []
    with open(file, 'r') as f:
        for line in f.readlines():
            row = tuple(line.strip().split(','))
            all_kv_pairs.append(row)

    # key sorting
    all_kv_pairs = sorted(all_kv_pairs, key=lambda x: x[0])

    key_values = {}
    current = ''
    # compiling
    for kv in all_kv_pairs:
        if current != kv[0]:
            current = kv[0]
            key_values[current] = []
        key_values[current].append(tuple([float(kv[1]), float(kv[2])]))

    final_tuples = []
    # reducing
    for key in key_values.keys():
        current_list = key_values[key]
        xcoord_list = []
        ycoord_list = []
        for row in current_list:
            xcoord_list.append(row[0])
            ycoord_list.append(row[1])
        xcord=sum(xcoord_list)/len(xcoord_list)
        ycord=sum(ycoord_list)/len(ycoord_list)
        final_tuples.append(list([key, xcord, ycord]))

    # writing to the output file
    out_file = f'{input}/../output/output{my_index}.txt'
    with open(out_file, 'a') as out:
        for row in final_tuples:
            tuple_str = ', '.join(map(str, row))
            out.write(f'{tuple_str}\n')
    out.close()

    return out_file
