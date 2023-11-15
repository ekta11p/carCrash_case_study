#!/usr/bin/env python
# coding: utf-8

# In[1]:


import yaml


# In[2]:


def read_yaml(file_path):
    """ read configuration file in yaml
    :parameter file_path: read config.yaml file
    :return : dictionary of config
    """
    with open(file_path,'r') as f:
        return yaml.safe_load(f)
    



def read_csv_file(spark,file_path):
    """ read input files from /data directory
    :parameter spark : spark instance
    :parameter file_path : input file path
    :return : dataframe 
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)




def write_output(output_df,file_path,file_format):
    """ save output to file
    :parameter output_df : output dataframe
    :parameter file_path: output file path
    :parameter file_format: output file format
    :return : None
    """
    output_df.write.format(file_format).mode('overwrite').option("header",True).save(file_path)
    
    
    


# In[ ]:




