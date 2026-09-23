package doudizhu

import "sort"

// fullDeck 返回一副 54 张牌（不含大小王为 52 张 + 2 王）。
// Rank: 3..15（3,4,...,A,2），16=小王，17=大王。
func fullDeck() []Card {
	deck := make([]Card, 0, 54)
	for suit := 0; suit < 4; suit++ {
		for rank := 3; rank <= 15; rank++ {
			deck = append(deck, Card{Suit: suit, Rank: rank, Joker: -1})
		}
	}
	deck = append(deck, Card{Rank: 16, Joker: 0}) // 小王
	deck = append(deck, Card{Rank: 17, Joker: 1}) // 大王
	return deck
}

// counts 按 Rank 计数（用于牌型识别）。
func rankCounts(cards []Card) map[int]int {
	m := make(map[int]int)
	for _, c := range cards {
		m[c.Rank]++
	}
	return m
}

// classify 识别牌型，返回牌型与主 Rank（用于比较）。非法返回 TypeInvalid。
func classify(cards []Card) (string, int) {
	n := len(cards)
	if n == 0 {
		return TypeInvalid, 0
	}
	m := rankCounts(cards)

	// 王炸
	if n == 2 && m[16] == 1 && m[17] == 1 {
		return TypeRocket, 17
	}
	// 单张
	if n == 1 {
		return TypeSingle, cards[0].Rank
	}
	// 对子
	if n == 2 {
		for r, c := range m {
			if c == 2 {
				return TypePair, r
			}
		}
	}
	// 炸弹（四张同点）优先于三张判定，避免被误判为三带一。
	if n == 4 {
		for r, c := range m {
			if c == 4 {
				return TypeBomb, r
			}
		}
	}
	// 三张 / 三带一 / 三带二
	if n >= 3 {
		for r, c := range m {
			if c >= 3 {
				rest := n - 3
				switch rest {
				case 0:
					return TypeTriple, r
				case 1:
					return TypeTriple1, r
				case 2:
					// 三带二：剩余 2 张须为一个对子
					for r2, c2 := range m {
						if r2 != r && c2 == 2 {
							return TypeTriple2, r
						}
					}
				}
			}
		}
	}
	// 顺子：5+ 张连续单牌，不含 2(15)/王(16,17)
	if n >= 5 {
		if allSingles(m) && isStraight(m, n) {
			return TypeStraight, maxRank(m)
		}
	}
	// 飞机：2+ 组连续三张（每组 3 张，无附带），不含 2/王
	if n >= 6 && n%3 == 0 {
		if isPlane(m, n/3) {
			return TypePlane, planeMainRank(m, n/3)
		}
	}
	return TypeInvalid, 0
}

func allSingles(m map[int]int) bool {
	for _, c := range m {
		if c != 1 {
			return false
		}
	}
	return true
}

func ranksSorted(m map[int]int) []int {
	rs := make([]int, 0, len(m))
	for r := range m {
		rs = append(rs, r)
	}
	sort.Ints(rs)
	return rs
}

// isStraight 连续 n 个单牌，不含 2/王（即最大 <=14，即 A）。
func isStraight(m map[int]int, n int) bool {
	rs := ranksSorted(m)
	if len(rs) != n {
		return false
	}
	for i := 1; i < len(rs); i++ {
		if rs[i] != rs[i-1]+1 {
			return false
		}
	}
	return rs[len(rs)-1] <= 14 // 不含 2
}

// isPlane k 组连续三张（k>=2），每组 Rank 连续且不含 2/王。
func isPlane(m map[int]int, k int) bool {
	groups := make([]int, 0)
	for r, c := range m {
		if c == 3 {
			groups = append(groups, r)
		} else {
			return false
		}
	}
	if len(groups) != k || k < 2 {
		return false
	}
	sort.Ints(groups)
	if groups[len(groups)-1] > 14 {
		return false
	}
	for i := 1; i < len(groups); i++ {
		if groups[i] != groups[i-1]+1 {
			return false
		}
	}
	return true
}

func planeMainRank(m map[int]int, k int) int {
	groups := make([]int, 0)
	for r, c := range m {
		if c == 3 {
			groups = append(groups, r)
		}
	}
	sort.Ints(groups)
	return groups[len(groups)-1]
}

func maxRank(m map[int]int) int {
	max := 0
	for r := range m {
		if r > max {
			max = r
		}
	}
	return max
}

// mainRankOf 取一组牌的主 Rank（出牌记录比较用）。
func mainRankOf(cards []Card) int {
	if len(cards) == 0 {
		return 0
	}
	_, r := classify(cards)
	return r
}

// canBeat 新出牌能否压过上家。
// 同型同张数才能比主 Rank；炸弹/王炸可压任意非炸弹/王炸。
func canBeat(newType string, newMain, newN int, lastType string, lastMain, lastN int) bool {
	if newType == TypeRocket {
		return true // 王炸最大
	}
	if lastType == TypeRocket {
		return false
	}
	if newType == TypeBomb {
		if lastType == TypeBomb {
			return newMain > lastMain
		}
		return true // 炸弹压非炸弹
	}
	if lastType == TypeBomb {
		return false
	}
	// 普通牌型：必须同型同张数，主 Rank 更大。
	if newType != lastType || newN != lastN {
		return false
	}
	return newMain > lastMain
}
