package main

import (
	"fmt"
	repo "gametaverse-data-service/repositories/functions"
)

func main() {
	fmt.Print(repo.GetBlockTransfer(14852202))
}
