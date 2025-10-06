# Frontend QA Checklist

These manual verification steps confirm that the campaign stage views render live data from the Andronoma backend APIs.

1. **Authenticate and launch a run**
   - Visit `/wizard`, register or log in, and launch a new run.
   - Wait for the pipeline to advance to later stages (monitor from `/console`).

2. **Audiences view** (`/audiences`)
   - Click **Refresh**. Confirm stage status updates and an audience table appears with persona rows populated from the latest CSV.
   - Verify the summary cards (segments/personas) reflect the run telemetry values and the table shows at least one row.

3. **Creatives view** (`/creatives`)
   - Click **Refresh**. Ensure concept metrics, tone highlights, and blocker coverage lists are populated from telemetry.
   - Validate the creative table renders headlines and associated metadata sourced from the generated CSV.

4. **Images view** (`/images`)
   - Click **Refresh**. Confirm provider, rendered/requested counts, and total cost update based on the images stage telemetry.
   - Verify that rendered image cards display concept IDs, providers, and storage paths pulled from the stage payload or asset registry.

5. **QA view** (`/qa`)
   - Click **Refresh**. Confirm the QA summary cards show totals/warnings/blockers and the checklist table lists real QA results with severity labels.
   - If any check includes remediation guidance, ensure it appears in the table.

6. **Export view** (`/export`)
   - Click **Refresh**. Verify bundle and manifest sizes populate, download links are rendered when signed URLs or storage keys are present, and export asset metadata is listed.

7. **Console regression** (`/console`)
   - Use **Refresh** to confirm runs load without TypeScript errors after the API typing changes.

If any view lacks data, confirm the corresponding pipeline stage has completed successfully and run the stage again from the console before re-testing.
