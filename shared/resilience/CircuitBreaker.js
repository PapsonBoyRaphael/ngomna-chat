/**
 * CircuitBreaker - Pattern de résilience pour protéger les appels externes
 * ✅ États : CLOSED → OPEN → HALF_OPEN → CLOSED
 * ✅ Fallback automatique quand le circuit est ouvert
 */

class CircuitBreaker {
  constructor(options = {}) {
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeout = options.resetTimeout || 30000;
    this.fallback = options.fallback || null;

    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = "CLOSED"; // CLOSED | OPEN | HALF_OPEN

    this.metrics = {
      totalCalls: 0,
      successfulCalls: 0,
      failedCalls: 0,
      fallbackCalls: 0,
      stateChanges: [],
    };
  }

  /**
   * Exécuter une opération avec protection du circuit breaker
   */
  async execute(operation) {
    this.metrics.totalCalls++;

    if (this.state === "OPEN") {
      if (Date.now() - this.lastFailureTime > this.resetTimeout) {
        this._changeState("HALF_OPEN");
      } else {
        if (this.fallback) {
          this.metrics.fallbackCalls++;
          console.warn("⚠️ Circuit ouvert, utilisation du fallback");
          return await this.fallback();
        }
        throw new Error("Circuit breaker ouvert");
      }
    }

    try {
      const result = await operation();

      if (this.state === "HALF_OPEN") {
        this._changeState("CLOSED");
        this.failureCount = 0;
      }

      this.metrics.successfulCalls++;
      return result;
    } catch (error) {
      this.failureCount++;
      this.lastFailureTime = Date.now();
      this.metrics.failedCalls++;

      if (this.failureCount >= this.failureThreshold) {
        this._changeState("OPEN");
        console.error(
          `❌ Circuit breaker ouvert après ${this.failureCount} échecs`
        );
      }

      throw error;
    }
  }

  /**
   * Changer l'état du circuit
   */
  _changeState(newState) {
    const oldState = this.state;
    this.state = newState;
    this.metrics.stateChanges.push({
      from: oldState,
      to: newState,
      timestamp: new Date().toISOString(),
    });
    console.log(`🔌 Circuit breaker: ${oldState} → ${newState}`);
  }

  /**
   * Réinitialiser le circuit breaker
   */
  reset() {
    this.failureCount = 0;
    this.lastFailureTime = null;
    this._changeState("CLOSED");
  }

  /**
   * Obtenir l'état actuel
   */
  getState() {
    return this.state;
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return {
      ...this.metrics,
      currentState: this.state,
      failureCount: this.failureCount,
      lastFailureTime: this.lastFailureTime,
    };
  }
}

module.exports = CircuitBreaker;
