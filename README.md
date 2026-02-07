# Connected Component Finder avec Apache Spark (RDD)

## 1. Objectif du projet

Ce projet implémente l'algorithme **Connected Component Finder (CCF)** pour identifier les composantes connexes d'un graphe de grande taille, dans un contexte Big Data. Deux variantes sont proposées :

- une version **basique** ;
- une version **optimisée** (mise en cache des RDD et réduction des recomputations).

L'objectif scientifique est double :

1. valider une implémentation correcte de CCF avec Apache Spark ;
2. comparer le comportement des deux variantes à travers une étude de **scalabilité**.

## 2. Approche globale et algorithmes

L'algorithme CCF fonctionne par propagation itérative d'une étiquette minimale à travers les sommets reliés.

- Le graphe est lu comme une liste d'arêtes `u v`.
- Les arêtes sont symétrisées pour traiter un graphe non orienté.
- À chaque itération, chaque sommet examine ses voisins et propage l'identifiant minimal observé.
- Le processus converge lorsqu'aucune nouvelle paire `(sommet, étiquette_minimale)` n'est créée.

Les deux versions partagent cette logique.
La version optimisée ajoute principalement des opérations de cache/unpersist pour limiter les recomputations d'un cycle à l'autre.

## 3. Structure du projet

```text
connected-components-spark/
├── data/
│   ├── small_graph.txt
│   ├── medium_graph.txt
│   └── large_graph.txt
├── experiments/
│   ├── run_experiments.py
│   └── results.csv
├── src/
│   ├── ccf_basic.py
│   ├── ccf_optimized.py
│   └── graph_generator.py
├── report.tex
├── requirements.txt
└── README.md
```

## 4. Prérequis

- **Python** >= 3.10 (recommandé : 3.11)
- **Apache Spark** >= 4.0
- Java installé et accessible (variable `JAVA_HOME` recommandée)

## 5. Installation (Python 3.11 + Spark 4.x)

### Étape 1 — Se placer à la racine du projet

```bash
cd /workspace/connected-components-spark
```

### Étape 2 — Créer et activer un environnement virtuel Python 3.11

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

### Étape 3 — Installer les dépendances Python

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Étape 4 — Vérifier Spark

```bash
spark-submit --version
```

## 6. Configuration d'environnement PySpark

Configurer explicitement l'interpréteur Python utilisé par le driver et les workers :

```bash
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)
```

Le projet est exécuté avec Spark en **mode local** (`--master local[*]`).

## 7. Génération des données

Le script ci-dessous génère trois graphes de tailles croissantes dans `data/` :

```bash
python src/graph_generator.py
```

Fichiers produits :

- `data/small_graph.txt`
- `data/medium_graph.txt`
- `data/large_graph.txt`

## 8. Exécution des algorithmes

### 8.1 Version basique

```bash
spark-submit --master local[*] src/ccf_basic.py
```

### 8.2 Version optimisée

```bash
spark-submit --master local[*] src/ccf_optimized.py
```

### 8.3 Évaluation expérimentale (scalabilité)

```bash
spark-submit --master local[*] experiments/run_experiments.py
```

## 9. Reproductibilité complète (ordre exact des commandes)

Exécuter les commandes suivantes, dans cet ordre :

```bash
cd /workspace/connected-components-spark
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)
python src/graph_generator.py
spark-submit --master local[*] src/ccf_basic.py
spark-submit --master local[*] src/ccf_optimized.py
spark-submit --master local[*] experiments/run_experiments.py
```

## 10. Fichiers de sortie

L'évaluation expérimentale produit :

- `experiments/results.csv`

Format attendu du CSV :

- `graph` : nom du graphe (`small`, `medium`, `large`)
- `time_basic` : temps d'exécution de la version basique (secondes)
- `time_optimized` : temps d'exécution de la version optimisée (secondes)

## 11. Interprétation synthétique des résultats expérimentaux

L'analyse de scalabilité compare les temps d'exécution lorsque la taille du graphe augmente.

- Les deux implémentations convergent vers des composantes connexes cohérentes.
- La version optimisée peut améliorer les performances lorsque les recomputations deviennent dominantes.
- En mode local, certains surcoûts Spark (ordonnancement, sérialisation, shuffle) peuvent limiter ou inverser le gain observé selon la taille des graphes.

Les résultats doivent donc être interprétés en tenant compte du contexte d'exécution : **Spark local** sur une machine unique, et non cluster distribué.
