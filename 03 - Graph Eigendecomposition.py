
# coding: utf-8

### Eigendecomposition for subset of PLD

#### Get the subset of the graph

# In this segment we load the dataset and keep only a tiny subset

##### Load first partition of PLD

# In[1]:

data = sc.textFile('small-pld-arc')


##### Keep only nodes with ID less than 5e4

# In[2]:

size = 5e4
small = data.map(
                   lambda line: [int(x) for x in line.split('\t')]
         ).filter(
                   lambda (source, destination): source<size and destination<size
         )


##### Persist result

# In[3]:

small.cache()


# In[4]:

small.count()


# In[5]:

small.take(10)


### Do the eigenvector calculation

#### Initialize First Iteration

# In[6]:

x1 = small.map(lambda (source, dest): (dest,1)).reduceByKey(add)


# In[7]:

x1.cache()
x1.count()


#### Exact iteration

# In[8]:

x2 = x1.join(small).map(lambda (k,v): (v[1], v[0])).reduceByKey(add)


# In[9]:

x2.cache()


# In[10]:

x2.count()


#### Third iteration

# In[11]:

x3 = x2.join(small).map(lambda (k,v): (v[1], v[0])).reduceByKey(add)


# In[12]:

x3.cache()


# In[13]:

x3.count()


# In[14]:

x3.filter(lambda (k,v): v>0).take(4)


#### Fourth Iteration

# In[15]:

x4 = x3.join(small).map(lambda (k,v): (v[1], v[0])).reduceByKey(add)


# In[16]:

x4.cache()


# In[17]:

x4.count()


#### `join()` with node names

# In[ ]:

x4.filter(lambda (k,v): v>10000).take(4)


# In[18]:

names = sc.textFile('small-pld-index')


# In[19]:

def indexline(line):
    parts = line.split('\t')
    return (int(parts[1]), parts[0])


# In[20]:

index = names.map(indexline).filter(lambda (k,v): k<size)


# In[21]:

index.cache()
index.takeSample(True, 10)


#### Show top domains

# In[36]:

topnodes = index.join(x4.filter(lambda (k,v): v>1e9))


# In[37]:

topnodes.cache()
topnodes.takeSample(True, 10)

