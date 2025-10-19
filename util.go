package producer

import (
	"math"
	"math/rand"
	"time"
)

func RandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func RandSleep(x float64) {
	base := 10.0
	mu := 1.0
	sigma := 0.1
	k := 0.8
	delay := math.Exp(-(math.Pow(x-mu, 2) / (2 * sigma * sigma))) / math.Sqrt(2*math.Pi*math.Pow(sigma, 2))
	time.Sleep(time.Duration(k*delay*100.0+base) * time.Millisecond)
}
