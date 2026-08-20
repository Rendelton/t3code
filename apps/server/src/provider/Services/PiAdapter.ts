/**
 * PiAdapter — shape type for the pi provider adapter.
 *
 * Naming anchor for the driver bundle ({@link ../Drivers/PiDriver}); the
 * adapter is a captured closure conforming to `ProviderAdapterShape`.
 *
 * @module PiAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface PiAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
