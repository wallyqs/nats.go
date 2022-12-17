package internal_test

// TC is a helper struct to lock creating TestClient into the repo.
type TC struct {
	// TC should implement the TestClient interface.
	TestClient
}

func (*TC) private() {}

type TestClient interface {
	SetConnectionStatus(int)
	// Ensures that no one can implement this interface,
	// unless the this interface type is embedded,
	// meaning that the code is able to reach the internal package.
	private()
}
