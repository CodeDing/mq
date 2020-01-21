
default:
	@CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o rabbit main.go
