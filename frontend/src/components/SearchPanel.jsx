import { useState, useEffect, useId } from 'react';
import { searchCities, compareJourney } from '../api/client';

/**
 * Panneau de recherche avec autocomplétion des villes.
 * Conforme RGAA : labels explicites, aria-autocomplete, role listbox/option,
 * navigation clavier complète dans les suggestions.
 */
export default function SearchPanel({ onResult }) {
  const fromId = useId();
  const toId   = useId();

  const [fromQuery,   setFromQuery]   = useState('');
  const [toQuery,     setToQuery]     = useState('');
  const [fromOptions, setFromOptions] = useState([]);
  const [toOptions,   setToOptions]   = useState([]);
  const [fromCity,    setFromCity]    = useState('');
  const [toCity,      setToCity]      = useState('');
  const [loading,     setLoading]     = useState(false);
  const [error,       setError]       = useState('');

  // Autocomplétion avec debounce
  useEffect(() => {
    if (fromQuery.length < 2) { setFromOptions([]); return; }
    const t = setTimeout(async () => {
      try {
        const data = await searchCities(fromQuery);
        setFromOptions(Array.isArray(data) ? data : (data.data ?? []));
      } catch { /* réseau indisponible — on ignore silencieusement */ }
    }, 300);
    return () => clearTimeout(t);
  }, [fromQuery]);

  useEffect(() => {
    if (toQuery.length < 2) { setToOptions([]); return; }
    const t = setTimeout(async () => {
      try {
        const data = await searchCities(toQuery);
        setToOptions(Array.isArray(data) ? data : (data.data ?? []));
      } catch { /* réseau indisponible */ }
    }, 300);
    return () => clearTimeout(t);
  }, [toQuery]);

  function selectFrom(city) {
    setFromCity(city.city);
    setFromQuery(city.city);
    setFromOptions([]);
  }

  function selectTo(city) {
    setToCity(city.city);
    setToQuery(city.city);
    setToOptions([]);
  }

  async function handleSubmit(e) {
    e.preventDefault();
    // La ville retenue = celle choisie dans les suggestions, sinon le texte saisi.
    const dep = (fromCity || fromQuery).trim();
    const arr = (toCity || toQuery).trim();
    if (!dep || !arr) {
      setError("Veuillez saisir une ville de départ et une ville d'arrivée.");
      return;
    }
    setError('');
    setLoading(true);
    try {
      const journey = await compareJourney(dep, arr);
      onResult({ journey, from: dep, to: arr });
    } catch {
      setError(`Aucun trajet trouvé entre « ${dep} » et « ${arr} ». Vérifiez l'orthographe ou essayez d'autres villes.`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <form
      className="search-form"
      onSubmit={handleSubmit}
      aria-label="Formulaire de comparaison de trajets ferroviaires et aériens"
      noValidate
    >
      <div className="form-row">
        <CityInput
          id={fromId}
          label="Ville de départ"
          placeholder="ex : Paris"
          value={fromQuery}
          options={fromOptions}
          selectedCity={fromCity}
          onChange={(val) => { setFromQuery(val); setFromCity(''); }}
          onSelect={selectFrom}
          listId="from-suggestions"
        />

        <CityInput
          id={toId}
          label="Ville d'arrivée"
          placeholder="ex : Berlin"
          value={toQuery}
          options={toOptions}
          selectedCity={toCity}
          onChange={(val) => { setToQuery(val); setToCity(''); }}
          onSelect={selectTo}
          listId="to-suggestions"
        />

        <button
          type="submit"
          className="btn-primary"
          disabled={loading}
          aria-busy={loading}
        >
          {loading ? 'Calcul en cours…' : 'Comparer les CO₂'}
        </button>
      </div>

      {error && (
        <p className="form-error" role="alert" aria-live="assertive">
          {error}
        </p>
      )}
    </form>
  );
}

/** Sous-composant : champ de saisie avec liste de suggestions accessible */
function CityInput({ id, label, placeholder, value, options, selectedCity, onChange, onSelect, listId }) {
  function handleKeyDown(e, city) {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      onSelect(city);
    }
  }

  return (
    <div className="form-group">
      <label htmlFor={id}>{label}</label>
      <input
        id={id}
        type="text"
        role="combobox"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        autoComplete="off"
        aria-autocomplete="list"
        aria-controls={listId}
        aria-expanded={options.length > 0}
        aria-activedescendant={selectedCity ? `${listId}-${selectedCity}` : undefined}
      />
      {options.length > 0 && (
        <ul
          id={listId}
          className="suggestions"
          role="listbox"
          aria-label={`Suggestions pour ${label.toLowerCase()}`}
        >
          {options.map((city) => (
            <li
              key={city.city}
              id={`${listId}-${city.city}`}
              role="option"
              aria-selected={selectedCity === city.city}
              tabIndex={0}
              onClick={() => onSelect(city)}
              onKeyDown={(e) => handleKeyDown(e, city)}
            >
              {city.city}
              {city.country_code && (
                <span className="city-country" aria-label={`pays : ${city.country_code}`}>
                  {' '}· {city.country_code}
                </span>
              )}
              {city.has_train && (
                <span className="city-tag city-tag--train" aria-label="Desserte ferroviaire disponible">
                  🚆
                </span>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
