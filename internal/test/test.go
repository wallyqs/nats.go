package internal_test

// TC is a helper struct to lock creating TestClient into the repo.
// TC should implement the TestClient interface.
// type TC struct {
// 	TestClient
// }

// func (TC) private() {}

type Engine interface {
	SetConnectionStatus(int)
	// Ensures that no one can implement this interface,
	// unless the this interface type is embedded,
	// meaning that the code is able to reach the internal package.
	// private()
}

// func TestClient(conn interface{}) TC {
// 	return conn.(TC)
// }

