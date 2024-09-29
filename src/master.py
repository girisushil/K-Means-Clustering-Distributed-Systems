import os
import sys
import json
import time

sys.path.append("..")
import grpc
from port import *
from concurrent import futures
from threading import Thread
from threading import Lock
import proto.map_reduce_pb2_grpc as servicer
from proto.map_reduce_pb2_grpc import MasterServicer, add_MasterServicer_to_server
import proto.map_reduce_pb2 as messages
from google.protobuf import empty_pb2
from pydantic import BaseModel
from pathlib import Path
import random
from time import sleep
import glob

def remove_file_dump():
    if os.path.exists("/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/dump.txt"):
        os.remove("/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/dump.txt")
        print(f"File has been successfully removed.")
    else:
        print(f"File does not exist.")

def remove_files_in_directory(directory):
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            os.remove(filepath)
            print(f"Removed file: {filepath}")


def read_coordinates_from_file(file_path):
    with open(file_path, 'r') as file:
        coordinates = []
        for line in file:
            if line.strip() == 'points':
                break
            x, y = map(float, line.strip().split(','))
            coordinates.append((x, y))
        return coordinates


def write_coordinates_to_files(coordinates, chunk_size):
    num_chunks = len(coordinates) // chunk_size
    remainder_points = len(coordinates) % chunk_size  # Calculate the number of remaining points
    for i in range(num_chunks):
        chunk = coordinates[i * chunk_size: (i + 1) * chunk_size]
        with open(
                f'/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/user_input/input_single_file/master_generated_{i + 1}.txt',
                'w') as file:
            for point in chunk:
                file.write(f"{point[0]}, {point[1]}\n")

    # Handle remaining points
    if remainder_points > 0:
        remainder_chunk = coordinates[num_chunks * chunk_size:]
        with open(
                f'/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/user_input/input_single_file/master_generated_{num_chunks + 1}.txt',
                'w') as file:
            for point in remainder_chunk:
                file.write(f"{point[0]}, {point[1]}\n")


def write_divide_file_master(input_file, chunk_size):
    coordinates = read_coordinates_from_file(input_file)
    write_coordinates_to_files(coordinates, chunk_size)


class Config(BaseModel):
    input_path: str
    output_path: str
    n_reducers: int
    n_mappers: int
    k: int
    iterations: int


class Master(MasterServicer):
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        # appending the name if the nodes
        self.num_mapper_notified = 0
        self.num_reducer_notified = 0
        self.node_path_list = []
        self.current_mapper_address = []
        self.current_mapper_paths = []
        self.current_reducer_paths = []
        self.current_reducer_address = []
        self.centroids_X = []
        self.centroids_Y = []
        self.output_file_combine = []
        self.final_iter_centeroids = []

        for i in range(1, 11):
            self.node_path_list.append(f'workers/worker{i}')

    def save_data_to_file(self,data):
        with open("/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/dump.txt", 'a') as file:
            file.write(data)
            file.write('\n')
    def read_input_data(self, file_path):
        data = []
        with open(file_path, 'r') as file:
            for line in file:
                # Split each line into values and convert them to float
                values = [float(x) for x in line.strip().split(',')]
                data.append(values)
        return data

    def initialize_centroids(self, data):
        # Initialize centroids randomly from the input data
        centroids = random.sample(data, self.config.k)
        for centroid in centroids:
            # Extract x and y coordinates
            x, y = centroid
            # Append x and y coordinates to their respective lists
            self.centroids_X.append(x)
            self.centroids_Y.append(y)

    def transfer_mapper(self, num_mapper: int):
        nodes = random.sample(self.node_path_list, num_mapper)
        map_path = os.getcwd() + '/functions/map.py'
        mapper_path = os.getcwd() + '/mapper.py'
        for node in nodes:
            dest_path = f'{os.getcwd()}/../{node}/mapper/'
            self.current_mapper_paths.append(dest_path)
            os.system(f'cp {mapper_path} {dest_path}')
            os.system(f'cp {map_path} {dest_path}')

    def invoke_mappers(self):
        for mapper in self.current_mapper_paths:
            port = get_new_port()
            print(f'Invoking mapper at location: {mapper}')
            s=f'Invoking mapper at location: {mapper}'
            self.save_data_to_file(s)
            self.current_mapper_address.append(f'localhost:{port}')
            os.system(f'python3 {mapper}mapper.py {port} &')
            sleep(0.5)

    def divide_input_files(self, input_path):
        num_mapper = self.config.n_mappers
        path_to_send = [messages.NotifyMapper() for i in range(num_mapper)]
        input_files = glob.glob(f'{input_path}/*.txt')
        for i in range(len(input_files)):
            path_to_send[i % num_mapper].input_paths.append(input_files[i])
        return path_to_send

    def start_mapper(self):
        path_to_send = self.divide_input_files(self.config.input_path)
        for i, mapper_addr in enumerate(self.current_mapper_address):
            mapper = grpc.insecure_channel(mapper_addr)
            notify_mapper_stub = servicer.MapperStub(mapper)
            path_to_send[i].num_reducer = self.config.n_reducers
            path_to_send[i].my_index = (i + 1)
            path_to_send[i].xcoord.extend(self.centroids_X)
            path_to_send[i].ycoord.extend(self.centroids_Y)
            response = notify_mapper_stub.StartMapper(
                path_to_send[i]
            )

    def start_reducer(self):
        intermediate_path = [f'{path}output' for path in self.current_mapper_paths]
        for i, reducer_addr in enumerate(self.current_reducer_address):
            messages_to_send = messages.NotifyReducer()
            messages_to_send.my_index = (i + 1)
            messages_to_send.num_mapper = self.config.n_mappers
            messages_to_send.intermediate_paths.extend(intermediate_path)

            reducer = grpc.insecure_channel(reducer_addr)
            notify_mapper_stub = servicer.ReducerStub(reducer)
            response = notify_mapper_stub.StartReducer(
                messages_to_send
            )

    def clear_mappers(self):
        print('[WARNING] clearing all the mappers')
        # clearing the mappers
        for mapper in self.current_mapper_paths:
            os.system(f'rm -rf {mapper}mapper.py')
            os.system(f'rm -rf {mapper}map.py')
            os.system(f'rm -rf {mapper}input/*.txt')
            os.system(f'rm -rf {mapper}output/*.txt')
        self.current_mapper_paths.clear()
        self.current_mapper_address.clear()

    def transfer_reducer(self, num_reducers: int):
        nodes = random.sample(self.node_path_list, num_reducers)
        reduce_path = os.getcwd() + '/functions/reduce.py'
        reducer_path = os.getcwd() + '/reducer.py'
        for node in nodes:
            dest_path = f'{os.getcwd()}/../{node}/reducer/'
            self.current_reducer_paths.append(dest_path)
            os.system(f'cp {reducer_path} {dest_path}')
            os.system(f'cp {reduce_path} {dest_path}')

    def invoke_reducer(self):
        for reducer in self.current_reducer_paths:
            port = get_new_port()
            print(f'Invoking reducer at location: {reducer}')
            s=f'Invoking reducer at location: {reducer}'
            self.save_data_to_file(s)
            self.current_reducer_address.append(f'localhost:{port}')
            os.system(f'python3 {reducer}reducer.py {port} &')
            sleep(0.5)

    def invoke_reducer_with_retry(self, max_attempts=3):
        for attempt in range(max_attempts):
            try:
                self.invoke_reducer()
                break  # If successful, exit the loop
            except Exception as e:
                print(f"Failed to invoke reducer (attempt {attempt + 1}/{max_attempts}): {e}")
                s="Failed to invoke reducer (attempt {attempt + 1}/{max_attempts}): {e}"
                self.save_data_to_file(s)
                if attempt < max_attempts - 1:
                    print("Retrying...")
                else:
                    print("Max attempts reached. Reassigning task to other reducers.")
                    # Reassign task to other reducers
                    self.clear_reducer()
                    self.handle_reducer()
                    break

    def invoke_mappers_with_retry(self, max_attempts=3):
        for attempt in range(max_attempts):
            try:
                self.invoke_mappers()
                break  # If successful, exit the loop
            except Exception as e:
                print(f"Failed to invoke mappers (attempt {attempt + 1}/{max_attempts}): {e}")
                s=f"Failed to invoke mappers (attempt {attempt + 1}/{max_attempts}): {e}"
                self.save_data_to_file(s)
                if attempt < max_attempts - 1:
                    print("Retrying...")
                    time.sleep(2 ** attempt)
                else:
                    print("Max attempts reached. Reassigning task to other mappers.")
                    # Reassign task to other mappers
                    self.clear_mappers()
                    self.handle_mappers()
                    break

    def clear_reducer(self):
        print('[WARNING] clearing all the reducers')
        # clearing the mappers
        for reducer in self.current_reducer_paths:
            os.system(f'rm -rf {reducer}reducer.py')
            os.system(f'rm -rf {reducer}reduce.py')
            os.system(f'rm -rf {reducer}input/*.txt')
            os.system(f'rm -rf {reducer}output/*.txt')
            os.system(f'rm -rf {self.config.output_path}/*.txt')
        self.current_reducer_paths.clear()
        self.current_reducer_address.clear()

    def collect_outputs(self):
        for path in self.current_reducer_paths:
            os.system(f'cp {path}output/output*.txt {self.config.output_path}/')

    def handle_mappers(self):
        # tranfering required number of mappers to the nodes
        self.transfer_mapper(self.config.n_mappers)
        self.invoke_mappers_with_retry()
        self.start_mapper()

    def handle_reducer(self):
        # tranfering required number of reducers to the nodes
        self.transfer_reducer(self.config.n_reducers)
        self.invoke_reducer_with_retry()
        self.start_reducer()

    def final_centeroids_value(self):
        combined_list = list(zip(self.centroids_X, self.centroids_Y))
        return combined_list

    def start(self):
        try:
            print("STARTING MASTER WITH THE FOLLOWING CONFIGURATIONS: ")
            print(f"Input Location = {self.config.input_path}" +
                  f"\nOutput Location = {self.config.output_path}" +
                  f"\nMappers = {self.config.n_mappers}\nReducers = {self.config.n_reducers}")

            thread = Thread(target=self.handle_mappers)
            thread.start()

            master_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            add_MasterServicer_to_server(self, master_server)
            print("MASTER STARTED")
            self.save_data_to_file("Master Started")
            master_server.add_insecure_port("localhost:8880")
            master_server.start()
            master_server.wait_for_termination(5)
        except KeyboardInterrupt:
            master_server.stop(None)
            print("----------CLOSING MASTER---------")
            return

    def NotifyMaster(self, request, context):
        print('NOTIFIED MASTER OF COMPLETION')
        if request.response == 0:
            self.num_mapper_notified += 1
        if request.response == 1:
            self.num_reducer_notified += 1
            self.output_file_combine.append(request.outfinal_paths)
        if self.num_mapper_notified == self.config.n_mappers:
            self.num_mapper_notified = 0
            thread = Thread(target=self.handle_reducer)
            thread.start()
        if self.num_reducer_notified == self.config.n_reducers:
            self.num_reducer_notified = 0
            self.collect_outputs()
            print('ALL DONE')
        return empty_pb2.Empty()

    def combine_files(self, folder_path):
        combined_points = {}
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    for line in file:
                        parts = line.strip().split(',')
                        key = parts[0]
                        point1 = float(parts[1])
                        point2 = float(parts[2])
                        if key not in combined_points:
                            combined_points[key] = []
                        combined_points[key].append((point1, point2))

        # Sort the points by key
        sorted_combined_points = {k: sorted(v) for k, v in sorted(combined_points.items())}
        return sorted_combined_points

    def update_centeroid_points(self):
        self.centroids_Y.clear()
        self.centroids_X.clear()
        combine_output = self.combine_files(self.config.output_path)
        print(combine_output)
        for k in combine_output.keys():
            if len(combine_output[k]) > 1:
                xfin = 0
                yfin = 0
                final_x = 0
                final_y = 0
                for i in range(len(combine_output[k])):
                    xfin += combine_output[k][i][0]
                    yfin += combine_output[k][i][1]
                    final_x = xfin / len(combine_output[k])
                    final_y = yfin / len(combine_output[k])
                self.centroids_X.append(final_x)
                self.centroids_Y.append(final_y)
            else:
                self.centroids_X.append(combine_output[k][0][0])
                self.centroids_Y.append(combine_output[k][0][1])

        print(self.centroids_X)
        print(self.centroids_Y)

    def dump_list_to_file(self, data_list, file_path):
        with open(file_path, 'w') as file:
            json.dump(data_list, file)


def main():
    input_path = input("Enter the input data location: ")
    # input_path = '/home/dscd/map-reduce/src/user_input/nj'
    output_path = input("Enter the output data location: ")
    # if output_path=='':
    #     output_path = '/home/dscd/map-reduce/src/user_output'
    num_mappers = int(input("Enter the number of mappers: "))
    num_reducers = int(input("Enter the number of reducers: "))
    k = int(input("Enter Number of Clusters: "))
    iterations = int(input("Enter Number of Iterations: "))

    input_data = Config(
        input_path=input_path,
        output_path=output_path,
        n_mappers=num_mappers,
        n_reducers=num_reducers,
        k=k,
        iterations=iterations
    )
    remove_file_dump()
    remove_files_in_directory("/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/user_input/input_single_file")
    write_divide_file_master("/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/point.txt", 4)
    master = Master(config=input_data)
    master.clear_mappers()
    master.clear_reducer()
    data = master.read_input_data("/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/point.txt")
    master.initialize_centroids(data)

    for i in range(iterations):
        print(f"Currently running iterations number: {i + 1}")
        s=f"Currently running iterations number: {i + 1}"
        master.save_data_to_file(s)
        master.start()
        master.update_centeroid_points()
        print("Clearing both mapper and reducer files for new iterations")
        if i == (iterations - 1):
            break
        master.clear_mappers()
        master.clear_reducer()

    print("After doing all the iterations the fina converged values of centeroids are: ")

    s = master.final_centeroids_value()
    print(s)
    print("Genrating Centeroids.txt file containing final centeroids")
    master.dump_list_to_file(s, "/Users/sushilhome/Desktop/DSCD_Assignment@3_fin/src/centeroids.txt")


if __name__ == "__main__":
    main()
