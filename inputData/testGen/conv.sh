echo "condata"
tr -d '\t' < convdata.csv > convdata2.csv
# tr -d ' ' < convdata2.csv > convdata3.csv
rm -f convdata.csv
# rm -f convdata2.csv
mv convdata2.csv convdata.csv
cat convdata.csv

