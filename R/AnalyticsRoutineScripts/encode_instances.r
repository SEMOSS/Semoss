encode_instances<-function(df,outfile){
# Objective - encode column instances 
# Arguments
# df - dataframe of one column containing maked data
# outfile - output file name for encoded instances 
out<-array()
n<-nrow(df)
minhash <- minhash_generator(n = 1, seed = 12231)
for(i in 1:n){
out[i]<-c(toString(minhash(toString(df[i,1]))))
}
write.table(out,file=outfile,sep="\t", col.names = F, row.names = F,quote = F)
}


