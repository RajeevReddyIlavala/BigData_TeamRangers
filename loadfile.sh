echo "script Started"
rm -rf datasets.tsv

hadoop fs -get /user/hm74/NYCOpenData/datasets.tsv
IFS = "\t"
cat datasets.tsv | while read line ; do 
read -a name <<< "$line"
filename="${name}.tsv.gz"
echo "$filename"
done 

