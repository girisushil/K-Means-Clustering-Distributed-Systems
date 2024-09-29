import glob
import numpy as np

def find_nearest_centroid(point, centroids):
    distances = [np.linalg.norm(np.array(point) - np.array(centroid)) for centroid in centroids]
    return np.argmin(distances)


def map(my_index, file_path, n_reducers, xcoord, ycoord):
    centeroids = list(zip(xcoord, ycoord))
    print(f'Mapper Index: {my_index}\tPath: {file_path}')
    input_files_paths = glob.glob(f"{file_path}/*.txt")
    values = []

    for index in range(n_reducers):
        with open(f"{file_path}/../output/output{index + 1}.txt", "a") as inter:
            inter.close()

    for file in sorted(input_files_paths):
        with open(file, 'r') as f:
            for line in f.readlines():
                point = [float(x) for x in line.strip().split(',')]
                nearest_centroid_index = find_nearest_centroid(point, centeroids)
                values.append([nearest_centroid_index, point])

    for obj in values:
        index = partitioning_function(obj[0], n_reducers)
        with open(f"{file_path}/../output/output{index+ 1}.txt", "a") as inter:
            inter.write(f"{obj[0]},{obj[1][0]}, {obj[1][1]}\n")
            inter.close()

    values.clear()


def partitioning_function(key, n_reducers):

    return key % n_reducers
