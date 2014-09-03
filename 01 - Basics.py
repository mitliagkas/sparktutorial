
# coding: utf-8

### A Spark context should already have been created

#### The context gives us access to all the Spark functionality

# In[1]:

sc


#### Feeding Spark data: via a method

##### Local copy: Data not yet "on Spark"

# In[2]:

local_data = range(100)


##### `parallelize()` makes an RDD out of our local data 

# In[3]:

distributed_data = sc.parallelize(local_data)
distributed_data


#### Sum of all odds

##### Use a filter operation.

# 
# Use python lambdas for anonymous functions.

# In[4]:

odds = distributed_data.filter(
            lambda x: x % 2 == 1
)


# No actual computation has happened yet. We're **just describing transformations**.

# In[8]:

print odds.toDebugString()


##### An action: `sum()`

# **Lazy calculation**: Spark will only run jobs and calculate stuff just now. 

# In[9]:

odds.sum()


##### Another action: `take()`

# In[10]:

distributed_data.take(10)

