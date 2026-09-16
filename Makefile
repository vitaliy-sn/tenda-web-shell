.PHONY: test test-race test-verbose test-cmd vet run build clean

# Run all tests.
test:
	go test ./...

# Run all tests with the race detector.
test-race:
	go test -race ./...

# Run all tests verbosely (shows each test's log output, e.g. the WS message trace).
test-verbose:
	go test -race -v ./...

# Run only the connect->command->response test (the one that performs the
# wget callback POST and verifies the command output arrives over WebSocket).
test-cmd:
	go test -race -v -run TestConnectCommandAndResponse .

# Static analysis / vet.
vet:
	go vet ./...

# Build the binary.
build:
	go build -o tendashell .

# Run the server (default flags; override with DEVICE=PORT=ADVERTISE=).
run: build
	./tendashell $(ARGS)

# Remove build artifacts.
clean:
	rm -f tendashell
