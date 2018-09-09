import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt


df = pd.read_excel('InOutNov.xlsx')
shivam_login_date = df.Date[df.EmpName == "Shivam Gupta"]
shivam_login_date = [str(i)[8:10] for i in shivam_login_date]
shivam_hrs = df.Total[df.EmpName == "Shivam Gupta"]
# y_pos = np.arange(len(shivam_login_date))
# plt.bar(y_pos, shivam_hrs, align='center', alpha=0.5)
# plt.xticks(y_pos, shivam_login_date)
# plt.ylabel('Hours')
# plt.xlabel("Date of Month")
# plt.title('In-Out Timings')
# plt.show()

shivam_hrs = shivam_hrs[7:]
puneet_hrs = df.Total[df.EmpName == "Puneet Aggarwal"]
print(len(shivam_hrs), len(puneet_hrs))
# data to plot
n_groups = 15

 
# create plot
fig, ax = plt.subplots()
index = np.arange(n_groups)
bar_width = 0.35
opacity = 0.8
rects1 = plt.bar(index, shivam_hrs, bar_width, alpha=opacity, color='b', label='Shivam')
rects2 = plt.bar(index + bar_width, puneet_hrs, bar_width, alpha=opacity, color='g', label='Puneet')
plt.xlabel('Person')
plt.ylabel('Hours')
plt.title('Hours by person')
plt.xticks(index + bar_width, shivam_login_date[7:])
plt.legend()
plt.tight_layout()
plt.show()