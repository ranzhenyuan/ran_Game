package doudizhu

import "testing"

func c(rank int) Card { return Card{Suit: 0, Rank: rank, Joker: -1} }

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		cards []Card
		want string
	}{
		{"single", []Card{c(5)}, TypeSingle},
		{"pair", []Card{c(5), c(5)}, TypePair},
		{"triple", []Card{c(5), c(5), c(5)}, TypeTriple},
		{"triple_one", []Card{c(5), c(5), c(5), c(3)}, TypeTriple1},
		{"triple_two", []Card{c(5), c(5), c(5), c(3), c(3)}, TypeTriple2},
		{"bomb", []Card{c(5), c(5), c(5), c(5)}, TypeBomb},
		{"rocket", []Card{{Rank: 16, Joker: 0}, {Rank: 17, Joker: 1}}, TypeRocket},
		{"straight", []Card{c(3), c(4), c(5), c(6), c(7)}, TypeStraight},
		{"plane", []Card{c(3), c(3), c(3), c(4), c(4), c(4)}, TypePlane},
		{"invalid_mix", []Card{c(3), c(5)}, TypeInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ, _ := classify(tc.cards)
			if typ != tc.want {
				t.Fatalf("classify=%s want %s", typ, tc.want)
			}
		})
	}
}

func TestCanBeat(t *testing.T) {
	// 炸弹压单张
	if !canBeat(TypeBomb, 5, 4, TypeSingle, 17, 1) {
		t.Fatal("bomb should beat single")
	}
	// 单张压不过炸弹
	if canBeat(TypeSingle, 17, 1, TypeBomb, 5, 4) {
		t.Fatal("single should not beat bomb")
	}
	// 王炸压炸弹
	if !canBeat(TypeRocket, 17, 2, TypeBomb, 15, 4) {
		t.Fatal("rocket should beat bomb")
	}
	// 同型同数，主Rank更大
	if !canBeat(TypeSingle, 10, 1, TypeSingle, 5, 1) {
		t.Fatal("higher single should beat lower")
	}
	// 不同型普通牌不能压
	if canBeat(TypePair, 10, 2, TypeSingle, 5, 1) {
		t.Fatal("pair should not beat single (non-bomb)")
	}
}

func TestFullDeckCount(t *testing.T) {
	if got := len(fullDeck()); got != 54 {
		t.Fatalf("deck size = %d want 54", got)
	}
}
