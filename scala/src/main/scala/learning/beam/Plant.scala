package learning.beam

import cats._
import magnolify.cats.semiauto._

// In Python Beam, plants were plain dicts:
//   {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
//
// In Scala, we use a case class — this gives us:
//   - Type safety (no typos in key names)
//   - Pattern matching
//   - Automatic Scio Coder derivation (serialization for distributed processing)
case class Plant(icon: String, name: String, duration: String)

object Plant {

  // Magnolify can automatically derive Cats type class instances for case classes.
  // It does this by combining instances for each field (here, all String fields).
  //
  // This is where Magnolify shines: instead of writing boilerplate implementations,
  // it generates lawful instances via Magnolia macro derivation.

  // Eq[Plant] — lawful equality, safer than == for generic/polymorphic code
  implicit val eqPlant: Eq[Plant] = EqDerivation[Plant]

  // Show[Plant] — type-safe toString alternative, useful for debugging output
  implicit val showPlant: Show[Plant] = ShowDerivation[Plant]

  // Note: Magnolify can also derive Semigroup, Monoid, etc. but those don't make
  // semantic sense for Plant (what does "combining" two plants mean?).
  //
  // In real Scio pipelines, other Magnolify modules become very useful:
  //   - magnolify-avro:     case class <-> Avro GenericRecord
  //   - magnolify-bigquery:  case class <-> BigQuery TableRow
  //   - magnolify-protobuf: case class <-> Protobuf Message

  // Sample data matching the Python notebook
  val gardeningPlants: Seq[Plant] = Seq(
    Plant("🍓", "Strawberry", "perennial"),
    Plant("🥕", "Carrot", "biennial"),
    Plant("🍆", "Eggplant", "perennial"),
    Plant("🍅", "Tomato", "annual"),
    Plant("🥔", "Potato", "perennial")
  )

  // Same data but with Potato having uppercase "PERENNIAL" — used in the
  // Iterator side input example to demonstrate filtering exclusion
  val gardeningPlantsWithTypo: Seq[Plant] = Seq(
    Plant("🍓", "Strawberry", "perennial"),
    Plant("🥕", "Carrot", "biennial"),
    Plant("🍆", "Eggplant", "perennial"),
    Plant("🍅", "Tomato", "annual"),
    Plant("🥔", "Potato", "PERENNIAL") // uppercase — won't match "perennial"
  )
}
