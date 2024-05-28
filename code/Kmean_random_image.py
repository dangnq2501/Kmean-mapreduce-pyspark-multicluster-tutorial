from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat_ws
from pyspark.sql.types import ArrayType, StringType
import cv2
import matplotlib.pyplot as plt

conf = SparkConf().setAppName("Kmean_RandomImage").setMaster("local[*]")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

image_path = '/home/k64t/work_logs/MiscStuff/Parallel_Kmean/data/img2.jpg'

image = cv2.imread(image_path)
image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
image_array = np.array(image)
reshaped_array = image_array.reshape(image_array.shape[0] * image_array.shape[1], -1)

#make random shit for testing purpose only
#reshaped_array = np.random.rand(10, 3)

data = pd.DataFrame(reshaped_array)
data_list = [tuple(row) for row in data.values]
data_rdd = sc.parallelize(data_list)

def MapRed_kmean_loss(centroids):
    def nearest_centroid(point):
        clusters = centroids.value
        distances = [np.linalg.norm(point - cluster) for cluster in clusters]
        return np.min(distances)
    
    return data_rdd.map(nearest_centroid).sum()

def MapRed_random_init_kmean(num_cluster = 5, max_iterations = 100):
    centroids = sc.broadcast(np.array(data.sample(n = num_cluster)))
    for iteration in range(max_iterations):
        # Map Step: Assign each point to the nearest centroid
        # key = c, value = (x, 1)
        closest = data_rdd.map(lambda x: (np.argmin([np.linalg.norm(x - c) for c in centroids.value]), (x, 1)))

        # Reduce Step: Compute sum of points and count for each centroid
        # sums = closest.reduceByKey(lambda x, y: ([x[0][i] + y[0][i] for i in range(len(x[0]))], x[1] + y[1]))
        sums = closest.reduceByKey(lambda x, y: (np.add(x[0], y[0]).astype(np.float64), x[1] + y[1]))

        # Compute new centroids
        new_centroids = sums.map(lambda x: (x[0], [x[1][0][i] / x[1][1] for i in range(len(x[1][0]))])).collect()

        centroids = sc.broadcast(np.array([c[1] for c in new_centroids]))
    
    return centroids, MapRed_kmean_loss(centroids)

def try_numcluster(num_cluster):
    max_try = 10
    best_loss = 1e12
    final_centroids = sc.broadcast(np.array(data.sample(n = num_cluster)))

    print(f'Current number of cluster : {num_cluster}----------------------')
    for i in range(0, max_try):
        centroids, loss = MapRed_random_init_kmean(num_cluster = num_cluster, max_iterations = 20)
        if (loss < best_loss):
            best_loss = loss
            final_centroids = centroids
        
        print(f'Iteration {i + 1} | loss : {loss} | best_loss {best_loss}')

    print("Final centroids:")
    for centroid in final_centroids.value:
        print(centroid)

    def get_nearest_centroid(point):
        clusters = centroids.value
        distances = [np.linalg.norm(point - cluster) for cluster in clusters]
        return np.argmin(distances)

    def print_new_image(centroids):
        
        closest = data_rdd.map(lambda x: (centroids.value[np.argmin([np.linalg.norm(x - c) for c in centroids.value])]))
        
        new_image = np.floor(np.array(closest.collect()))
        new_image = new_image.astype(np.uint8)
        new_image = new_image.reshape(image_array.shape[0], image_array.shape[1], -1)

        output_path = '/home/k64t/work_logs/MiscStuff/Parallel_Kmean/output_image/img2_' + str(num_cluster) + '_res.jpg'
        plt.imsave(output_path, new_image)
        
    print_new_image(final_centroids)

for num in [2, 5, 10, 15, 20]:
    try_numcluster(num)

sc.stop()