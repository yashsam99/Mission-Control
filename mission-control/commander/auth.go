package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// signToken issues an HS256 JWT valid for ttl, with subject set to the
// requesting Soldier instance's id.
func signToken(secret []byte, ttl time.Duration, subject string) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject:   subject,
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		IssuedAt:  jwt.NewNumericDate(now),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}

// validateToken returns the token's claims only if the token is well-formed,
// HS256-signed with secret, not expired, and carries a subject claim that is
// a well-formed instance id (a UUID). The subject requirement exists because
// every token this system issues is scoped to a Soldier instance — a token
// with no subject, or a malformed one, is not a token this system issued.
func validateToken(secret []byte, tokenString string) (*jwt.RegisteredClaims, error) {
	claims := &jwt.RegisteredClaims{}
	_, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		func(*jwt.Token) (any, error) { return secret, nil },
		jwt.WithValidMethods([]string{"HS256"}),
	)
	if err != nil {
		return nil, err
	}
	if claims.Subject == "" {
		return nil, errors.New("token missing subject claim")
	}
	if _, err := uuid.Parse(claims.Subject); err != nil {
		return nil, fmt.Errorf("token subject is not a valid instance id: %w", err)
	}
	return claims, nil
}
