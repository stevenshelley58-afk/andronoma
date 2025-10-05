const creatives = [
  {
    headline: "Accelerate go-to-market with orchestrated intelligence",
    body: "Andronoma unifies scrape → process → audiences → creatives → images → QA → export into a single control plane.",
    cta: "Request demo",
  },
  {
    headline: "Deploy campaigns in hours, not weeks",
    body: "Automated QA and budget enforcement keep teams confident while moving fast.",
    cta: "See platform",
  },
];

export default function Creatives() {
  return (
    <div className="card">
      <h2>Creative Variations</h2>
      <p>Review copy exploration coming out of the generation stage.</p>
      <div className="stage-grid">
        {creatives.map((creative, index) => (
          <div key={index} className="stage-card">
            <h3>{creative.headline}</h3>
            <p>{creative.body}</p>
            <span className="badge">CTA: {creative.cta}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
