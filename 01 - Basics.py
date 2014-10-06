
# coding: utf-8

### https://github.com/mitliagkas/sparktutorial

### Start `pyspark`.

#### A Spark context should already have been created

#### The context gives us access to all the Spark functionality

# In[1]:

sc


#### Feeding Spark data: via a method

##### Local copy: Data not yet "on Spark"

# In[2]:

local_data = range(100)


##### `parallelize()` makes a Resilient Distributed Dataset (RDD) out of our local data 

# In[3]:

distributed_data = sc.parallelize(local_data)
distributed_data


#### Sum of all odds

##### Use a filter operation.

# 
# Use python lambdas for anonymous functions.

# In[5]:

def isodd(x):
    return x % 2 == 1

odds = distributed_data.filter( isodd )


# Alternatively:

# In[4]:

odds = distributed_data.filter(
            lambda x: x % 2 == 1
)


# No actual computation has happened yet. We're **just describing transformations**.

# In[6]:

print odds.toDebugString()


##### An action: `sum()`

# **Lazy calculation**: Spark will only run jobs and calculate stuff just now. 

# In[7]:

odds.sum()


##### Another action: `take()`

# In[8]:

distributed_data.take(10)


# In[ ]:



