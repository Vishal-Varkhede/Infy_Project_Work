# Databricks notebook source
# MAGIC %md
# MAGIC ## Configure Pipeline

# COMMAND ----------

#  my child's notebook runs correctly. Any thoughts why the status variable returns None
# print (status) and as a result I see None after printing. 
# I don't have dbutils.notebook.exit statement in the child notebook. So nothing is passed to the parent one. After I've updated the child I got the status in the parent

# COMMAND ----------

# Errors in workflows thrown a WorkflowException.
def run_with_retry(notebook, timeout):
    status = dbutils.notebook.run(notebook, timeout)
    return status
  
def status_step_check(status,step):
  if status == "Success":
    print(status, ' Step '+str(step)+' Completed')
    step = step+1
  else:
    raise Exception("Job failed at step "+str(step)) 
    
  return step

# COMMAND ----------

Creation_of_Bronze_And_Silver_Tables_From_Raw_Data = './Creation_of_Bronze_And_Silver_Tables_From_Raw_Data'
Creation_Of_Gold_Tables_From_Silver_Tables         = './Creation_Of_Gold_Tables_From_Silver_Tables'

# COMMAND ----------

# %run ../../../resources/configs/config_v3

# COMMAND ----------

try :
  step = int(dbutils.widgets.get("step"))
except Exception:
  step = 1

try :
  step_end = int(dbutils.widgets.get("step_end"))
except Exception:
  step_end = 2

print("start : ",step)
print("end   : ",step_end)

# COMMAND ----------

if step == 1:
  print('Running step 1 : Creation_of_Bronze_And_Silver_Tables_From_Raw_Data')
  status = run_with_retry(Creation_of_Bronze_And_Silver_Tables_From_Raw_Data, timeout=0)
  #status = dbutils.notebook.run(Main_Code,1200)
  # print(' Step 1 Completed')
  print(status, ' Step 1 Completed')
  step = step+1
else:
    raise Exception("Job failed at step 1")
      

if step == 2 and step_end>=2:      
  print('Running step 2 : Creation_Of_Gold_Tables_From_Silver_Tables')
  status = run_with_retry(Creation_Of_Gold_Tables_From_Silver_Tables, timeout=0)
  step = status_step_check(status,step)
    

# COMMAND ----------


