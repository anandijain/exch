/-!
A small Lean sketch for the exchange core.

This is intentionally modest. The goal is to name the simple properties the Rust core should
preserve before we build a fuller executable model.
-/

inductive Side where
  | buy
  | sell
deriving DecidableEq, Repr

structure Order where
  id : Nat
  account : Nat
  side : Side
  price : Nat
  quantity : Nat
deriving DecidableEq, Repr

structure Level where
  price : Nat
  quantity : Nat
deriving DecidableEq, Repr

structure Book where
  bids : List Level
  asks : List Level
deriving DecidableEq, Repr

structure Balance where
  available : Nat
  reserved : Nat
deriving DecidableEq, Repr

def totalBalance (balance : Balance) : Nat :=
  balance.available + balance.reserved

def reserve (balance : Balance) (amount : Nat) : Option Balance :=
  if proof : amount ≤ balance.available then
    some {
      available := balance.available - amount
      reserved := balance.reserved + amount
    }
  else
    none

def strictlyDescendingPrices : List Level -> Prop
  | [] => True
  | [_] => True
  | a :: b :: rest => a.price > b.price ∧ strictlyDescendingPrices (b :: rest)

def strictlyAscendingPrices : List Level -> Prop
  | [] => True
  | [_] => True
  | a :: b :: rest => a.price < b.price ∧ strictlyAscendingPrices (b :: rest)

def positiveQuantities (levels : List Level) : Prop :=
  levels.all (fun level => level.quantity > 0) = true

def uncrossed (book : Book) : Prop :=
  match book.bids, book.asks with
  | bid :: _, ask :: _ => bid.price < ask.price
  | _, _ => True

def validBook (book : Book) : Prop :=
  strictlyDescendingPrices book.bids ∧
  strictlyAscendingPrices book.asks ∧
  positiveQuantities book.bids ∧
  positiveQuantities book.asks ∧
  uncrossed book

theorem emptyBookValid : validBook { bids := [], asks := [] } := by
  repeat constructor

theorem reservePreservesTotal
    (balance : Balance)
    (amount : Nat)
    (updated : Balance)
    (h : reserve balance amount = some updated) :
    totalBalance updated = totalBalance balance := by
  unfold reserve at h
  by_cases hle : amount ≤ balance.available
  · simp [hle] at h
    injection h with hupdated
    rw [← hupdated]
    unfold totalBalance
    omega
  · simp [hle] at h
