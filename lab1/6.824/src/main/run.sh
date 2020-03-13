go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrsequential.go wc.so pg*.txt


go run mrmaster.go pg-*.txt

go run mrworker.go wc.so

cat mr-out-* | sort | more