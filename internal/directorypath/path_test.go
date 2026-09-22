package directorypath

import "testing"

// TestNormalize is the table-driven behavior matrix for logical directory
// normalization, including the Locus-compatible "pop at root" behavior.
func TestNormalize(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "", want: "/"},
		{name: "root", input: "/", want: "/"},
		{name: "single segment", input: "archive", want: "/archive"},
		{name: "nested segments", input: "archive/2024", want: "/archive/2024"},
		{name: "leading slash", input: "/archive/2024", want: "/archive/2024"},
		{name: "trailing slash", input: "archive/2024/", want: "/archive/2024"},
		{name: "duplicate separators", input: "archive//2024", want: "/archive/2024"},
		{name: "windows separators", input: `archive\2024\01`, want: "/archive/2024/01"},
		{name: "current directory segments", input: "archive/./2024", want: "/archive/2024"},
		{name: "parent pops segment", input: "archive/2024/../2023", want: "/archive/2023"},
		{name: "parent at root is ignored", input: "../archive", want: "/archive"},
		{name: "parents above root collapse", input: "../../", want: "/"},
		{name: "only parent segments", input: "..", want: "/"},
		{name: "only dot segment", input: ".", want: "/"},
		{name: "surrounding whitespace", input: "  archive/2024  ", want: "/archive/2024"},
		{name: "unicode segments", input: "租户/子目录", want: "/租户/子目录"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Normalize(tc.input); got != tc.want {
				t.Errorf("Normalize(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestNormalize_IsIdempotent verifies that normalizing an already normalized
// path is a no-op.
func TestNormalize_IsIdempotent(t *testing.T) {
	inputs := []string{"/", "/archive", "/archive/2024/01", "../archive", "archive/../../x"}

	for _, input := range inputs {
		once := Normalize(input)
		if twice := Normalize(once); twice != once {
			t.Errorf("Normalize(Normalize(%q)) = %q, want %q", input, twice, once)
		}
	}
}
