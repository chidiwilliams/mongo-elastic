package mongo

var systemDBNames = []string{"admin", "config", "local"}

// IsSystemDB returns true if s is the name of a MongoDB system database.
// https://docs.mongodb.com/manual/reference/system-collections/
func IsSystemDB(s string) bool {
	for _, name := range systemDBNames {
		if name == s {
			return true
		}
	}
	return false
}
