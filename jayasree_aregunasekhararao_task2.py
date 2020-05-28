from pyspark import SparkContext
from collections import defaultdict
import time
import sys
import itertools
import math

def frequent(baskets,prev,flat,support,k):
	count={}
	freq_items=[]
	# print("suppppppoooooooooooooooooooooooooooooooooort", support)
	if k==1:
		candidate=[]
		for basket in baskets:
			for b in basket:
				if b not in candidate: candidate.append(b)

		for cand in candidate:
			# print("CCCCCCCAAAAAAAANNNNNNNNNDDDDDDDDDDDDDDDDDDDD",cand)
			for basket in baskets:
				# print("BBBBBBBBBBBBBBaAskkkkkkkkkeeeeeeeeeeeeeeettttttttttttt", basket)
				if cand in basket:
					if cand not in count:
						count[cand]=1
						# if count[cand]>=support:
						# 	freq_items.append(cand)
					else:
						count[cand]=count[cand]+1
					
					if count[cand]>=support:
						freq_items.append((cand,))
						break

		# for fk,fv in count.items():
		# 	if fv>=support:
		# 		freq_items.append(fk)

		flat=[]
		for f in freq_items:
			for fi in f:
				flat.append(fi)
		# print('im in frequent 1s, im returning ',sorted(freq_items))
		return sorted(freq_items),flat

	elif k==2:
		candidate=[tuple(sorted(i)) for i in itertools.combinations(flat,k)]
		#list(itertools.comb/....)
		# print('candidates for k=2,', candidate)
		# print('im basket',baskets)

	else:

		candidate=[]
		i=list(itertools.combinations(flat,k))
		for item in i:
			# print('items for k=3,',item)
			flag=1
			for j in range(k):
				# for v in range(k):
				# 	if v!=j:
				# 		if item[v] not in prev:
				# 			flag=0
				# 			break
				check=sorted(item[:j]+item[j+1:])
				# print("check", check)
				if tuple(check) not in prev:
					flag=0
					break
			if flag==1:
				candidate.append(tuple(sorted(item)))
		# print('candidates for k=3,',candidate)
		# print('im baskets,',baskets)

	# print("CAAAAAAAAAAnddddddidateeeeeeeeeeee", candidate)
	# ans=[]
	# for i,cand in enumerate(candidate):
	# 	a=[]
	# 	ans.append(a)
	# print('Im going to start enumearte, candidate[0],')
	for i,cand in enumerate(candidate):
		# print("CCCCCCCAAAAAAAANNNNNNNNNDDDDDDDDDDDDDDDDDDDD",cand)
		for basket in baskets:
			
			# print("BBBBBBBBBBBBBBaAskkkkkkkkkeeeeeeeeeeeeeeettttttttttttt", basket)
			#for c in cand:
				# print("CCCCCCCccccccccccccccccccccccccc",c)

			if set(cand).issubset(set(basket)):
				# print("TTTTTTTTTTrrrrrrrrruuuuuuueeeeeeee")
				# ans[i].append(1)
				if cand not in count:
					count[cand]=1
					# if count[cand]>=support:
					# 	freq_items.append(cand)
				else:
					count[cand]=count[cand]+1
				
				if count[cand]>=support:
					freq_items.append(cand)
					break
	# print('im done with enumrate')

	#for i,cand in enumerate(candidate):	
		# print("CCCCCCCAAAAAAAANNNNNNNNNDDDDDDDDDDDDDDDDDDDD222222222222",set(cand))

	# 	if sum(ans[i])>=support:
	# 		freq_items.append(cand)
	# print('checking count dictionary,',count)
	# print('my freq_items till now', freq_items)
	# print("CCCCCCCCCCcounnnnnnnnnnnnnnnnnttttttttttttttttttttttttt", count)
	# for ck,cv in count.items():
	# 	if cv>=support:
	# 		freq_items.append(ck)

	flat=[]
	for f in freq_items:
		for fi in f:
			flat.append(fi)

	# print("FREWUENNNNNNNNNTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT",freq_items)
	# print('after checking count, freq_items',freq_items)
	return sorted(freq_items), flat


def apriori(myiterator):
	iterator=list(myiterator)
	s=math.ceil(support*len(iterator))
	baskets=[]
	# print("IIITTTTTTEEEEEERRRRRRRAAAATTTTTTTTTTRRRRRRRRRRRRRRRRRRRRr", iterator)

	for i in iterator:
		# print("iiiiiiiiiiiiiiiiiiiiiiiiiiii", i)
		baskets.append(set(i))
	# print("BASSSSSSSLETTTTTTTTTTTt", baskets)
	prev=[]
	freq=[]
	k=1
	flag=1
	candidates=[]
	flat=[]
	i=0
	while(flag==1):

		curr, flat=frequent(baskets,prev,flat, s,k)
		# print("CUUUUUUUUUURRRRRRRRRRRRRRRRRRRRRrrrrr",curr)
		if(len(curr)==0):
			flag=0
		else:
			candidates.append(curr)
			for c in curr:
				freq.append(c)
		k=k+1
		prev=curr
		flat=set(flat)

	# print("fieldddddddddddddddd", freq)
	yield freq

# def final_map(iterator):
# 	iter=list(iterator)
# 	map_count={}
# 	map_freq=[]
# 	# print("IIIIIIIIIterrrrrrrrrrrrrrrrr",iter)
# 	for it in iter:
# 		for i in it:
# 			# print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiii", i)
# 			for b in basks:
# 				# print("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", b)
# 				if set(i).issubset(set(b)):
# 					# print("TTTTTTTTTTTRRRRRRRRRRRRRRRRRRREEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
# 					if i in map_count:
# 						map_count[i]+=1
# 					else:
# 						map_count[i]=1
# 					if map_count[i]>=supp:
# 						map_freq.append(i)
# 						break

	

# 	yield map_freq

def myfunc(e):
	return (len(e),e)


if __name__=="__main__":

	# print("HELOOOOOOOOOOOOOOOOOOOOOOOOO")
	filt = int(sys.argv[1])
	supp=int(sys.argv[2])
	input_file=sys.argv[3]
	output_file=sys.argv[4]
	initial_time=time.time()
	sc= SparkContext('local[*]', 'Task1')

	sp_rdd= sc.textFile(input_file)
	head=sp_rdd.first()
	sp=sp_rdd.filter(lambda x: x!=head)
	data=sp.map(lambda x:x.split(",")).map(lambda x:(x[0],x[1])).groupByKey().map(lambda x: list(x[1])).filter(lambda x:len(x)>=filt)
	basks=data.collect()
	# print("DATAAAAAAAAAAAAAAAAAAAAAAAAA",data.collect())
	support=float(supp/len(basks))
	son_rdd=data.mapPartitions(apriori)
	son=son_rdd.flatMap(lambda x:x).collect()
	son=sorted(set(son),key=myfunc)
	#son2=son_rdd.mapPartitions(final_map).collect()
	# print("SONNNNNNNf",son)
	map_count={}
	map_freq=[]
	# print("IIIIIIIIIterrrrrrrrrrrrrrrrr",iter)
	for i in son:
		# print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiii", i)
		for b in basks:
			# print("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", b)
			if set(i).issubset(set(b)):
				# print("TTTTTTTTTTTRRRRRRRRRRRRRRRRRRREEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
				if i in map_count:
					map_count[i]+=1
				else:
					map_count[i]=1
				if map_count[i]>=supp:
					map_freq.append(i)
					break


	
	# freq_2=set(())
	# for y in son2:
	# 	for item in y:
	# 		freq_2.add(item)
	freq_2=set(map_freq)

	freq_2=sorted(freq_2, key=myfunc)
	# print("SONNNNNNNf",son)
	# print("SONNNNNNNffffffffffffffffrrrrrrrrrrrrrrrqqqqqqqqqqqqqqqq",freq_1)
	d_time=time.time()
	print("nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn", d_time-initial_time)
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

	# print("Candidateeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", son)
	# print("SONNNNNNNNNNNNNNNNNNNNN",son2)
	
	# print("SONNNNNNNffffffffffffffffrrrrrrrrrrrrrrrqqqqqqqqqqqqqqqq",freq_2)

	
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
	print("Durationnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn", final_time-initial_time)

