# Stress and Consistency Tests with Dynamic Fault Tolerance

This testing framework allow us to evaluate the fault tolerance and the system consistency during sress loads, with the possibility to kill services dynamically during the test's ramp-up phase (you can also set the recovery times for each killed service).

## Principal Characteristics

1. **Test di Consistenza** - Verifica che il sistema mantenga la consistenza dei dati quando i servizi falliscono
2. **Fault Tolerance Dinamica** - Uccide servizi in momenti specifici durante il test di stress
3. **Recovery Automatico** - Ripristina i servizi dopo un intervallo di tempo configurabile
4. **Visualizzazione dei Risultati** - Analizza e visualizza i risultati dei test

## Setup

### Requirements

* Python >= 3.8
* Docker
* Microservises must be running in Docker containers

### Intallation

1. install requirements
   ```bash
   pip install -r requirements.txt
   ```

2. Assicurati che i tuoi servizi siano in esecuzione in container Docker.

3. Verifica che gli URL in `urls.json` siano corretti per il tuo ambiente.

## Available Tests

### 1. Test di Stress con Fault Tolerance Dinamica

Questo test permette di uccidere servizi durante la fase di crescita del carico:

```bash
python run_dynamic_stress_test.py --users 1000 --spawn-rate 50 --runtime 300
```

Questo comando:
- Inizia un test di stress con 1000 utenti
- Aggiunge 50 nuovi utenti al secondo
- Esegue il test per 300 secondi
- Usa la configurazione di kill predefinita in `kill_config.json`

### 2. Test di Consistenza con Fault Tolerance

Il test di consistenza verifica che il sistema mantenga la consistenza dei dati quando i servizi falliscono:

```bash
python run_fault_tolerant_tests.py --test-type consistency --kill-services order-service
```

### 3. Test Combinato con Fault Tolerance Statica

Esegue entrambi i test di consistenza e stress:

```bash
python run_fault_tolerant_tests.py --test-type both --kill-services stock-service
```

## Workflow Example

1. Preparing the sest for dynamic stress:
   ```bash
   mkdir -p results visualizations
   
   # Modify kill_config.json (if needed)
   ```

2. Execute the sest with dynamic kill during ramp-up phase:
   ```bash
   python run_dynamic_stress_test.py --init --users 1000 --spawn-rate 50 --runtime 300
   ```

3. Results visualization (comiing soon):
   ```bash
   python visualize_results.py --report
   ```

## How Dynamic Service Killing Works

The dynamic kill mechanissm works indipendently during the Locust test file execution 

1. `DynamicServiceKiller` starts with the test

2. A background thread monitors metrics (users, time, RPS)

3. On threshold breach, it kills specified services

4. If recovery time is set, services are restarted automatically

This approach simulates realistic failure scenarios during high-load growth phases, helping test system resilience under pressure.