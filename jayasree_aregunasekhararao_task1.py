from pyspark import SparkContext
from collections import defaultdict
import time
import sys
import itertools
import math

def frequent(baskets,prev,flat,support,k):
	count={}
	freq_items=[]
	if k==1:
		candidate=[]
		for basket in baskets:
			for b in basket:
				if b not in candidate: candidate.append(b)

		for cand in candidate:
	
			for basket in baskets:
		
				if cand in basket:
					if cand not in count:
						count[cand]=1
		
					else:
						count[cand]=count[cand]+1
					
					if count[cand]>=support:
						freq_items.append((cand,))
						break





		flat=[]
		for f in freq_items:
			for fi in f:
				flat.append(fi)

		return sorted(freq_items),flat

	elif k==2:
		candidate=[tuple(sorted(i)) for i in itertools.combinations(flat,k)]




	else:

		candidate=[]
		i=list(itertools.combinations(flat,k))
		for item in i:
	
			flag=1
			for j in range(k):
		
		
		
		
		
				check=sorted(item[:j]+item[j+1:])
		
				if tuple(check) not in prev:
					flag=0
					break
			if flag==1:
				candidate.append(tuple(sorted(item)))



	for i,cand in enumerate(candidate):

		for basket in baskets:
			
	
	
		

			if set(cand).issubset(set(basket)):
		
		
				if cand not in count:
					count[cand]=1
			
			
				else:
					count[cand]=count[cand]+1
				
				if count[cand]>=support:
					freq_items.append(cand)
					break




	flat=[]
	for f in freq_items:
		for fi in f:
			flat.append(fi)

	return sorted(freq_items), flat


def apriori(myiterator):
	iterator=list(myiterator)
	s=math.ceil(support*len(iterator))
	baskets=[]

	for i in iterator:

		baskets.append(set(i))
	prev=[]
	freq=[]
	k=1
	flag=1
	candidates=[]
	flat=[]
	i=0
	while(flag==1):

		curr, flat=frequent(baskets,prev,flat, s,k)

		if(len(curr)==0):
			flag=0
		else:
			candidates.append(curr)
			for c in curr:
				freq.append(c)
		k=k+1
		prev=curr
		flat=set(flat)

	yield freq

	

def myfunc(e):
	return (len(e),e)


if __name__=="__main__":

	case = int(sys.argv[1])
	supp=int(sys.argv[2])
	input_file=sys.argv[3]
	output_file=sys.argv[4]
	initial_time=time.time()
	sc= SparkContext('local[*]', 'Task1')

	sp= sc.textFile(input_file)
	if case==1:
		data=sp.map(lambda x:x.split(",")).map(lambda x:(x[0],x[1])).groupByKey().map(lambda x: list(x[1]))
	if case==2:
		data=sp.map(lambda x:x.split(",")).map(lambda x:(x[1],x[0])).groupByKey().map(lambda x: list(x[1]))
	basks=data.collect()
	support=float(supp/len(basks))
	son_rdd=data.mapPartitions(apriori)
	son=son_rdd.flatMap(lambda x:x).collect()
	son=sorted(set(son),key=myfunc)
	map_count={}
	map_freq=[]
	for i in son:

		for b in basks:
	
			if set(i).issubset(set(b)):
		
				if i in map_count:
					map_count[i]+=1
				else:
					map_count[i]=1
				if map_count[i]>=supp:
					map_freq.append(i)
					break


	
	freq_2=set(map_freq)

	freq_2=sorted(freq_2, key=myfunc)
	d_time=time.time()
	
	file=open(output_file,"w+",encoding="utf-8")
	file.write("Candidates:\n")
	for index,row in enumerate(son):
		
		if index<len(son)-1 and len(row)!=len(son[index+1]):
			if len(row)==1:
				r=str(row)
				row=r[:-2]+")"
			file.write(str(row)+"\n\n")
		elif index==len(son)-1:
			if len(row)==1:
				r=str(row)
				row=r[:-2]+")"
			file.write(str(row)+"\n\n")
		else:
			if len(row)==1:
				r=str(row)
				row=r[:-2]+")"
			file.write(str(row)+",")

	

	
	file.write("Frequent Itemsets:\n")
	for index,row in enumerate(freq_2):
		
		if index<len(freq_2)-1 and len(row)!=len(freq_2[index+1]):
			if len(row)==1:
				r=str(row)
				row=r[:-2]+")"
			file.write(str(row)+"\n\n")
		elif index==len(freq_2)-1:
			if len(row)==1:
				r=str(row)
				row=r[:-2]+")"
			file.write(str(row))
		else:
			if len(row)==1:
				r=str(row)
				row=r[:-2]+")"
			file.write(str(row)+",")

	file.close() 

	final_time=time.time()
	print("\"Duration\": ", final_time-initial_time)	

