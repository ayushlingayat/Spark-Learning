#Simple Function in python
from functools import reduce


def my_sum(x , y):
    return x+y

total = my_sum(10 , 5)
print(total)


#<-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><->

my_list =[1,2,3,4,5,6,7]

def doubler(x):
    return 2*x

my_listdouble = list(map(doubler,my_list))
#we used map method here

print(my_listdouble)


#<-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><->

# instead of writing function explicitly
# we are declaring this here only because of lambda function okk just take in your mind

# now lambda function example

s1 = "Ayush Lingayat"

s2 = lambda func : func.upper()

print(s2(s1))

#using lambda function

output = list(map(lambda x: x*2 ,my_list))

print(output)

# function to sum the list

ordinary_list =[1,2,3,4,5,6,7,8]

print("This is the ordinary list" , ordinary_list)

def listSum(x):
    total = 0
    for i in x:
        total = total + i
    return total

print(listSum(ordinary_list))


reducer_output = (reduce(lambda x,y : x+y , ordinary_list))

print(reducer_output)

#directly see output of reduce
# from functools import reduce
# reducer_output = reduce(lambda x, y: x + y, ordinary_list)