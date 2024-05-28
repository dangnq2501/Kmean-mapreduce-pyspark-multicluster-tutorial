from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat_ws
from pyspark.sql.types import ArrayType, StringType
import cv2
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

conf = SparkConf().setAppName("Kmean_RandomImage").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data_path = '/home/k64t/work_logs/MiscStuff/Parallel_Kmean/data/apple_quality.csv'

df = pd.read_csv(data_path)
df = df.drop('A_id', axis = 1)
df['Quality'] = df['Quality'].map({'good': 1, 'bad': 0})

train_df, test_df = train_test_split(df, test_size = 0.3)

train_label = train_df['Quality'].values
train_data = train_df.drop('Quality',axis = 1)
test_label = test_df['Quality'].values
test_data = test_df.drop('Quality',axis = 1)

data_list = [tuple(row) for row in train_data.values]
data_rdd = sc.parallelize(data_list)

def MapRed_kmean_loss(centroids):
    def nearest_centroid(point):
        clusters = centroids.value
        distances = [np.linalg.norm(point - cluster) for cluster in clusters]
        return np.min(distances)
    
    return data_rdd.map(nearest_centroid).sum()

def MapRed_random_init_kmean(num_cluster = 5, max_iterations = 100):
    centroids = sc.broadcast(np.array(train_data.sample(n = num_cluster)))
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
    max_try = 20
    best_loss = 1e12
    final_centroids = sc.broadcast(np.array(train_data.sample(n = num_cluster)))

    print(f'Current number of cluster : {num_cluster}----------------------')
    for i in range(0, max_try):
        centroids, loss = MapRed_random_init_kmean(num_cluster = num_cluster, max_iterations = 35)
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

    cnt = np.zeros((num_cluster,2))
    closest = data_rdd.map(lambda x: (np.argmin([np.linalg.norm(x - c) for c in centroids.value])))
    for idx, cluster in enumerate(closest.collect()):
        cnt[cluster][train_label[idx]] += 1
    
    cluster_label = cnt.argmax(axis = 1)

    def evaluate(centroids, data, label):
        
        data_list = [tuple(row) for row in data.values]
        data_rd = sc.parallelize(data_list)
        
        closest_cluster_label = data_rd.map(lambda x: (cluster_label[np.argmin([np.linalg.norm(x - c) for c in centroids.value])]))
        predict = np.array(closest_cluster_label.collect())

        return accuracy_score(predict, label)

    print(f'Train dataset accuracy : {evaluate(final_centroids, train_data, train_label)}')
    print(f'Test dataset accuracy : {evaluate(final_centroids, test_data, test_label)}')

for num in [32, 64, 126, 256, 384]:
    try_numcluster(num)
 
sc.stop()
