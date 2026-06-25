//! Model-name routing.
//!
//! The request's `model` field selects the path:
//! - `moa/<recipe>` or `moa-auto`  -> MoA orchestration
//! - any name known to the registry -> passthrough to that upstream
//! - anything else                  -> unknown (404)
//!
//! `route()` is pure and does not need the registry for the MoA/unknown split;
//! the caller supplies a predicate telling whether a plain model name is known,
//! so this stays dependency-free and unit-testable.

/// Where a request should go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteTarget {
    /// Passthrough to a configured upstream model by name.
    Passthrough { model: String },
    /// MoA orchestration using the named recipe (`moa-auto` -> "auto").
    Moa { recipe: String },
    /// No route matches.
    Unknown { model: String },
}

/// MoA recipe prefix.
pub const MOA_PREFIX: &str = "moa/";
/// MoA auto alias.
pub const MOA_AUTO: &str = "moa-auto";

/// Route a model name. `is_known_model` answers whether a plain (non-MoA) name
/// resolves to a configured upstream.
pub fn route<F>(model: &str, is_known_model: F) -> RouteTarget
where
    F: Fn(&str) -> bool,
{
    if let Some(recipe) = model.strip_prefix(MOA_PREFIX) {
        if recipe.is_empty() {
            return RouteTarget::Unknown {
                model: model.to_string(),
            };
        }
        return RouteTarget::Moa {
            recipe: recipe.to_string(),
        };
    }
    if model == MOA_AUTO {
        return RouteTarget::Moa {
            recipe: "auto".to_string(),
        };
    }
    if is_known_model(model) {
        RouteTarget::Passthrough {
            model: model.to_string(),
        }
    } else {
        RouteTarget::Unknown {
            model: model.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_known_model_to_passthrough() {
        let known = |m: &str| m == "gpt-5.5";
        assert_eq!(
            route("gpt-5.5", known),
            RouteTarget::Passthrough {
                model: "gpt-5.5".into()
            }
        );
    }

    #[test]
    fn routes_moa_prefix_and_auto_to_moa() {
        let none = |_: &str| false;
        assert_eq!(
            route("moa/arm-e", none),
            RouteTarget::Moa {
                recipe: "arm-e".into()
            }
        );
        assert_eq!(
            route("moa-auto", none),
            RouteTarget::Moa {
                recipe: "auto".into()
            }
        );
    }

    #[test]
    fn routes_unknown_model() {
        let none = |_: &str| false;
        assert_eq!(
            route("nope", none),
            RouteTarget::Unknown {
                model: "nope".into()
            }
        );
        // empty recipe is not a valid MoA route
        assert_eq!(
            route("moa/", none),
            RouteTarget::Unknown {
                model: "moa/".into()
            }
        );
    }
}
