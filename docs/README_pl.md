# Ethereum Blockchain Analysis App

![GitHub](https://img.shields.io/github/license/grodekk/ether_blockchain)
![Python Version](https://img.shields.io/badge/python-3.x-blue)

Aplikacja służąca do kompleksowego pobierania, przetwarzania i analizowania danych z blockchaina Ethereum.  
Umożliwia śledzenie aktywności portfeli oraz analizę podstawowych danych transakcyjnych,   takich jak liczba transakcji i opłaty transakcyjne.  
Procesy w aplikacji mogą być sterowane ręcznie lub automatycznie.


## Spis Treści

- [Funkcjonalności](#funkcjonalności)
- [Architektura i Struktura Projektu](#architektura-i-struktura-projektu)
- [Technologie](#technologie)
- [Instalacja i Konfiguracja](#instalacja-i-konfiguracja)
- [Uruchomienie](#uruchomienie)
- [Planowane Ulepszenia](#planowane-ulepszenia)
- [Autor i Kontakt](#autor-i-kontakt)
- [Licencja](#licencja)

## Funkcjonalności

Projekt składa się z następujących modułów:

### Pobieranie bloków
- Pobiera dane z sieci Ethereum – na podstawie zakresu numerów bloków lub określonego okresu
- Zapisuje dane jako pliki JSON
- Wykorzystuje wieloprocesowość dla zoptymalizowanej wydajności

### Ekstrakcja danych
- Przetwarza surowe dane z bloków
- Wyodrębnia podstawowe dane transakcyjne (liczba transakcji, opłaty)
- Kategoryzuje portfele według sald
- Zapisuje przetworzone dane w formacie JSON

### Zapis danych topowych portfeli
- Sprawdza i aktualizuje historię wybranych portfeli
- Zapisuje dane o zmianach sald i szczegółach największych transakcji

### Zarządzanie bazą danych
- Importuje dane z plików JSON do bazy SQLite3
- Umożliwia centralizację i zaawansowaną analizę danych

### Zarządzanie wykresami
- Przystosowany do użycia w interfejsie graficznym
- Generuje interaktywne wykresy (Matplotlib i mplcursors)
- Umożliwia analizę trendów, wahań opłat i dystrybucji sald portfeli

### Interfejs użytkownika konsolowy
- Konsolowy interfejs (CLI) z pełną kontrolą nad procesami
- Planowany interfejs graficzny (PyQt5) - w fazie refaktoryzacji

### Moduł usuwający bloki
- Usuwa bloki na podstawie określonego przedziału czasowego lub numerów bloków
- Optymalizuje przechowywanie danych

### Interfejs użytkownika deskopowy
- Graficzny deskopowy interejs dla użytkownika do sterowania programem w pyqt5
- Obecnie w refaktoryzacji

### Moduły pomocnicze
- **Plik konfiguracyjny (config.py)**: Przechowuje ustawienia projektu
- **Files Checker**: Automatycznie sprawdza i tworzy wymagane katalogi oraz pliki
- **Logger**: Niestandardowy moduł logowania
- **Error Handler**: Centralna obsługa błędów

## Architektura i Struktura Projektu

```
ethereum-blockchain-analysis/
├── automation.py            # Automatyzacja aplikacji
├── blocks_download.py       # Pobieranie bloków z sieci Ethereum
├── blocks_extractor.py      # Ekstrakcja i analiza danych
├── database_tool.py         # Import danych z JSON do SQLite3
├── charts.py                # Wizualizacja danych
├── console.py               # Interfejs wiersza poleceń (CLI)
├── automation.py            # Moduł automatyzacji procesów
├── wallets_update.py        # Aktualizacja i monitorowanie portfeli
├── blocks_remover.py        # Usuwanie bloków
├── config.py                # Konfiguracja aplikacji
├── logger.py                # Moduł logowania
├── error_handler.py         # Obsługa błędów
├── files_checker.py         # Sprawdzanie i tworzenie katalogów
├── interface.py             # Interfejs graficzny (GUI)
├── README.md                # główny plik README odwołujący do pełnego README
├── docs/
│   ├── README.pl.md         # pełny README wersja polska
│   ├── LICENSE              # plik z licencją
│   └── requirements.txt     # lista zależności
└── tests/                   # Testy jednostkowe i integracyjne
```

## Technologie

- **Python 3.x**
- **Requests** - komunikacja z API Ethereum
- **Multiprocessing** - efektywne pobieranie bloków
- **PyQt5** - interfejs graficzny
- **SQLite3** - lokalna baza danych
- **Matplotlib & mplcursors** - wizualizacja danych
- **Schedule** - automatyzacja zadań
- **dotenv** - zarządzanie zmiennymi środowiskowymi
- **Pytest** - testy jednostkowe i integracyjne

## Instalacja i Konfiguracja

### Instalacja zależności

```bash
pip install -r requirements.txt
```

### Konfiguracja

Plik `.env` jest tworzony automatycznie przy pierwszym uruchomieniu aplikacji. Należy uzupełnić go własnym kluczem API Etherscan:

```
API_KEY=twój_klucz_api_etherscan
```

## Uruchomienie

Aplikację uruchamiasz z poziomu terminala:

```bash
python console.py
```

## Planowane Ulepszenia

- Rozszerzenie możliwości analitycznych o biblioteki NumPy i Pandas
- Dodanie wersji webowej użytkownika
- Dalsza optymalizacja wydajności przy przetwarzaniu dużych zbiorów danych
- Zwiększenie pokrycia testami oraz dalsza refaktoryzacja kodu zgodnie z zasadami SOLID

## Autor i Kontakt

**Autor**: Tomasz Grodecki

- **GitHub**: [https://github.com/grodekk]
- **LinkedIn**: [https://linkedin.com/in/3838ab263]
- **E-mail**: [mailto:grodecki.job@gmail.com]

## Podziękowania

Projekt był wspierany przez narzędzia AI, w tym ChatGPT, który pomógł w generowaniu kodu, rozwiązywaniu problemów i udzielaniu wskazówek podczas tworzenia aplikacji.

## Licencja

Ten projekt jest objęty licencją MIT. Szczegóły znajdują się w pliku [LICENSE](LICENSE).
