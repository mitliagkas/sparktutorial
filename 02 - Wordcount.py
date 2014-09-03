
# coding: utf-8

#### Load data from text file

# In[61]:

data = sc.textFile('iliad.mb.txt')


# In[62]:

print data.toDebugString()


# For text files, `textFile()` creates one RDD entry per line. Let us count the lines.

# In[63]:

data.count()


#### Count total number of words

# `map()` each line to an integer (word count).

# In[64]:

linecounts = data.map(
        lambda line: len(line.split(' ')) 
)


# In[65]:

linecounts.sum()


#### Calculate individual wordcounts

# `flatmap()` each line to a **list** of words.

# In[66]:

words = data.flatMap(
        lambda line: line.split(' ')
)


# `map()` each word to **list** of (word,1) pairs.

# In[67]:

word_pairs = words.map(
        lambda word: (word,1)
)


# `reduceByKey()` the **list** of (word,1) pairs to (word,count)

# In[68]:

from operator import add
word_counts = word_pairs.reduceByKey(add)


# No action taken yet! But we can see the DAG describing the tranformations leading to this RDD.

# In[69]:

print word_pairs.toDebugString()


#### Action: get results

# In[70]:

word_counts.take(7)

