package catalog

import "os"

func mkdirAll(p string) error  { return os.MkdirAll(p, 0o755) }
func writeFile(p string, b []byte) error { return os.WriteFile(p, b, 0o644) }
