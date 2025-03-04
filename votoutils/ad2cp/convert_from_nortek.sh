echo $1
echo $3
cd $2
pwd
/usr/bin/java -jar "$1" -N  -v -c -r -f "$3"
