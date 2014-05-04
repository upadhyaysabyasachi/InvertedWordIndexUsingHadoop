An inverted index file us created  from an xml file containing urls, keywords and their weights. 
The xmk file is parsed, fed into a Map-Reduce program to extract the keys as keywords, and then put 
through Comparators, Partitioners and Secondary sorting to create 27=26+1 different files, each file containing
a list of keywords starting with a particular alphabet and their corrspeonding urls, ranked according to the weight and occurence.
with the 27th file containing keywords starting with non-alphabets.


secC - secondary sorting
