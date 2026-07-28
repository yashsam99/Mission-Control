package main

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// signToken issues an HS256 JWT valid for ttl.
func signToken(secret []byte, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		IssuedAt:  jwt.NewNumericDate(now),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}

// validateToken returns nil only if the token is well-formed, HS256-signed
// with secret, and not expired.
func validateToken(secret []byte, tokenString string) error {
	_, err := jwt.ParseWithClaims(
		tokenString,
		&jwt.RegisteredClaims{},
		func(*jwt.Token) (any, error) { return secret, nil },
		jwt.WithValidMethods([]string{"HS256"}),
	)
	return err
}
