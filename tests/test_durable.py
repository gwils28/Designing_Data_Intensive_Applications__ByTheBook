"""Tests du mini-moteur d'exécution durable."""
import pytest

from ddia.durable import Crash, Engine, ExternalService, NonDeterminismError, crash_once, crash_randomly


def make_payment(card, bank):
    def fraud(amount):
        return amount > 1000

    def workflow(ctx, amount):
        if ctx.activity(fraud, amount):
            return "fraude"
        ctx.activity(card, amount, name="debit", with_key=True)
        ctx.activity(bank, amount, name="depot", with_key=True)
        return "ok"
    return workflow


def test_sans_panne():
    card, bank = ExternalService("carte"), ExternalService("banque")
    eng = Engine()
    assert eng.start("w1", make_payment(card, bank), 50) == "ok"
    assert len(card.effects) == 1 and len(bank.effects) == 1 and eng.restarts == 0


def test_panne_avant_le_depot_rejeu_sans_double_debit():
    card, bank = ExternalService("carte"), ExternalService("banque")
    eng = Engine(crash=crash_once("before", "depot"))
    assert eng.start("w1", make_payment(card, bank), 50) == "ok"
    assert eng.restarts == 1 and len(card.effects) == 1 and len(bank.effects) == 1
    naive = Engine(crash=crash_once("before", "depot"), durable=False)
    card2, bank2 = ExternalService("carte"), ExternalService("banque")
    naive.start("w1", make_payment(card2, bank2), 50)
    assert len(card2.effects) == 2


def test_fenetre_apres_effet_et_idempotence():
    card, bank = ExternalService("carte"), ExternalService("banque")
    Engine(crash=crash_once("after", "debit")).start("w", make_payment(card, bank), 50)
    assert len(card.effects) == 2                       # effet refait : pas encore journalisé
    card2, bank2 = ExternalService("carte", idempotent=True), ExternalService("banque", idempotent=True)
    Engine(crash=crash_once("after", "debit")).start("w", make_payment(card2, bank2), 50)
    assert len(card2.effects) == 1 and card2.calls == 2


def test_pannes_aleatoires_exactement_une_fois_avec_idempotence():
    card, bank = ExternalService("carte", idempotent=True), ExternalService("banque", idempotent=True)
    eng = Engine(crash=crash_randomly(0.3, seed=4), max_restarts=10_000)
    for k in range(200):
        eng.start(f"w{k}", make_payment(card, bank), 10)
    assert len(card.effects) == 200 and len(bank.effects) == 200 and eng.restarts > 50


def test_non_determinisme_detecte():
    card, bank = ExternalService("carte"), ExternalService("banque")
    eng = Engine(crash=crash_once("before", "depot"))
    eng.store["w"] = {"history": [], "args": (50,), "version": "v1", "done": False, "result": None, "attempts": 0}
    eng.resume("w", {"v1": make_payment(card, bank)}, stop_after=1)
    assert not eng.store["w"]["done"]

    def reordered(ctx, amount):
        ctx.activity(card, amount, name="debit", with_key=True)
        ctx.activity(lambda a: False, amount, name="fraud")
        return "ok"
    with pytest.raises(NonDeterminismError):
        eng.resume("w", {"v1": reordered})
    assert eng.resume("w", {"v1": make_payment(card, bank)}) == "ok"


def test_horloge_et_hasard_deterministes():
    seen = []

    def wf(ctx):
        r = ctx.random()
        seen.append(r)
        ctx.activity(lambda: None, name="a")
        return r
    eng = Engine(crash=crash_once("after", "a"), seed=1)
    out = eng.start("w", wf)
    assert len(seen) == 2 and seen[0] == seen[1] == out
