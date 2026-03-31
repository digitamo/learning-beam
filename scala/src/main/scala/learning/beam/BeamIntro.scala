package learning.beam

import cats.syntax.all._
import com.spotify.scio._
import com.spotify.scio.values.SCollection

// =============================================================================
// Scio translation of the Python Apache Beam notebook (beam-intro.ipynb).
//
// Scio is Spotify's Scala API for Apache Beam. It wraps Beam's Java SDK with
// idiomatic Scala — SCollection instead of PCollection, .map/.filter instead of
// ParDo/DoFn boilerplate.
//
// Each method below corresponds to a cell in the Python notebook.
// =============================================================================
object BeamIntro {

  def main(args: Array[String]): Unit = {
    basicFilterAndMap()
    filterWithClosedOverVariable()
    filterWithSingletonSideInput()
    filterWithIterableSideInput()
    filterWithMapSideInput()
    magnolifyCatsDemo()
  }

  // ---------------------------------------------------------------------------
  // Example 1: Basic filter + map with debug output
  // ---------------------------------------------------------------------------
  // Python:
  //   p | beam.Create(range(1, 11))
  //     | beam.Filter(lambda num: num % 2 == 0)
  //     | "filtered output" >> Output(prefix='PCollection filtered value: ')
  //     | beam.Map(lambda num: num * 2)
  //     | "doubled output" >> Output(prefix='PCollection double value: ')
  //
  // In Python, we needed a custom Output PTransform (a class with an inner DoFn)
  // just to print elements and pass them through. In Scio, .debug() does this
  // out of the box — it prints each element with an optional prefix and yields
  // the element unchanged.
  //
  // Key translations:
  //   beam.Create(...)  ->  sc.parallelize(...)
  //   beam.Filter(fn)   ->  .filter(fn)
  //   beam.Map(fn)      ->  .map(fn)
  //   Output(prefix=x)  ->  .debug(prefix = x)
  // ---------------------------------------------------------------------------
  def basicFilterAndMap(): Unit = {
    println("\n=== Example 1: Basic filter + map ===")

    val (sc, _) = ContextAndArgs(Array("--runner=DirectRunner"))

    sc.parallelize(1 to 10)
      .filter(_ % 2 == 0)
      .debug(prefix = "PCollection filtered value: ")
      .map(_ * 2)
      .debug(prefix = "PCollection doubled value: ")

    sc.run().waitUntilDone()
  }

  // ---------------------------------------------------------------------------
  // Example 2: Filter with a closed-over variable
  // ---------------------------------------------------------------------------
  // Python:
  //   beam.Filter(lambda plant, duration: plant['duration'] == duration, 'perennial')
  //
  // In Python Beam, extra arguments to Filter are forwarded to the lambda. This
  // is needed because Python lambdas don't naturally close over pipeline-time
  // values.
  //
  // In Scala, closures capture variables from the enclosing scope naturally.
  // We just reference `duration` directly — no special mechanism needed.
  // ---------------------------------------------------------------------------
  def filterWithClosedOverVariable(): Unit = {
    println("\n=== Example 2: Filter with closed-over variable ===")

    val (sc, _) = ContextAndArgs(Array("--runner=DirectRunner"))

    val duration = "perennial"

    sc.parallelize(Plant.gardeningPlants)
      // Scala closures naturally capture `duration` from the enclosing scope.
      // No need for Python's extra-arg forwarding pattern.
      .filter(_.duration == duration)
      .debug()

    sc.run().waitUntilDone()
  }

  // ---------------------------------------------------------------------------
  // Example 3: Filter with Singleton side input
  // ---------------------------------------------------------------------------
  // Python:
  //   perennial = p | beam.Create(['perennial'])
  //   ... | beam.Filter(
  //       lambda plant, duration: plant['duration'] == duration,
  //       duration=beam.pvalue.AsSingleton(perennial))
  //
  // Side inputs let one PCollection be used as a "lookup" inside another's
  // transform. A Singleton side input expects exactly one element.
  //
  // In Scio:
  //   1. Create the side input:  val si = collection.asSingletonSideInput
  //   2. Attach it:              .withSideInputs(si)
  //   3. Access via context:     ctx(si) returns the single value
  //   4. Convert back:           .toSCollection
  //
  // The withSideInputs pattern gives you an SCollectionWithSideInput, where
  // .filter/.map take (element, context) => ... instead of just element => ...
  // ---------------------------------------------------------------------------
  def filterWithSingletonSideInput(): Unit = {
    println("\n=== Example 3: Filter with Singleton side input ===")

    val (sc, _) = ContextAndArgs(Array("--runner=DirectRunner"))

    // This PCollection has exactly one element, so it can be a Singleton
    val durationSI = sc.parallelize(Seq("perennial")).asSingletonSideInput

    sc.parallelize(Plant.gardeningPlants)
      .withSideInputs(durationSI)
      .filter { case (plant, ctx) =>
        // ctx(durationSI) returns the single String "perennial"
        plant.duration == ctx(durationSI)
      }
      .toSCollection // back to a regular SCollection[Plant]
      .debug()

    sc.run().waitUntilDone()
  }

  // ---------------------------------------------------------------------------
  // Example 4: Filter with Iterable side input
  // ---------------------------------------------------------------------------
  // Python:
  //   valid_durations = p | beam.Create(['annual', 'biennial', 'perennial'])
  //   ... | beam.Filter(
  //       lambda plant, valid_durations: plant['duration'] in valid_durations,
  //       valid_durations=beam.pvalue.AsIter(valid_durations))
  //
  // AsIter/asIterableSideInput makes the entire PCollection available as an
  // Iterable. Useful when you need to check membership against a small set.
  //
  // Note: Potato has "PERENNIAL" (uppercase) to show it gets excluded — the
  // iterable only contains lowercase values.
  // ---------------------------------------------------------------------------
  def filterWithIterableSideInput(): Unit = {
    println("\n=== Example 4: Filter with Iterable side input ===")

    val (sc, _) = ContextAndArgs(Array("--runner=DirectRunner"))

    val validDurationsSI = sc
      .parallelize(Seq("annual", "biennial", "perennial"))
      .asIterableSideInput

    sc.parallelize(Plant.gardeningPlantsWithTypo) // Potato has "PERENNIAL"
      .withSideInputs(validDurationsSI)
      .filter { case (plant, ctx) =>
        // ctx(validDurationsSI) returns Iterable[String]
        // We convert to Set for efficient lookup
        val validDurations = ctx(validDurationsSI).toSet
        validDurations.contains(plant.duration)
      }
      .toSCollection
      .debug()

    sc.run().waitUntilDone()
  }

  // ---------------------------------------------------------------------------
  // Example 5: Filter with Map side input
  // ---------------------------------------------------------------------------
  // Python:
  //   keep_duration = p | beam.Create([
  //       ('annual', False), ('biennial', False), ('perennial', True)])
  //   ... | beam.Filter(
  //       lambda plant, keep_duration: keep_duration[plant['duration']],
  //       keep_duration=beam.pvalue.AsDict(keep_duration))
  //
  // AsDict/asMapSideInput converts a PCollection of key-value pairs into a Map.
  // The PCollection must be of type (K, V) — Scio enforces this at compile time.
  //
  // This is useful when the filtering logic is data-driven (the keep/drop
  // decision comes from another PCollection, not hardcoded).
  // ---------------------------------------------------------------------------
  def filterWithMapSideInput(): Unit = {
    println("\n=== Example 5: Filter with Map side input ===")

    val (sc, _) = ContextAndArgs(Array("--runner=DirectRunner"))

    // PCollection[(String, Boolean)] — required type for asMapSideInput
    val keepDurationSI = sc
      .parallelize(Seq(
        ("annual", false),
        ("biennial", false),
        ("perennial", true)
      ))
      .asMapSideInput

    sc.parallelize(Plant.gardeningPlants)
      .withSideInputs(keepDurationSI)
      .filter { case (plant, ctx) =>
        // ctx(keepDurationSI) returns Map[String, Boolean]
        ctx(keepDurationSI).getOrElse(plant.duration, false)
      }
      .toSCollection
      .debug()

    sc.run().waitUntilDone()
  }

  // ---------------------------------------------------------------------------
  // Example 6: Magnolify Cats type class derivation demo
  // ---------------------------------------------------------------------------
  // This doesn't have a Python equivalent — it demonstrates Magnolify's value.
  //
  // Magnolify automatically derives Cats type class instances (Eq, Show, etc.)
  // for case classes by combining instances of each field. No boilerplate needed.
  //
  // In real Scio pipelines, Magnolify is most often used for:
  //   - magnolify-avro:     case class <-> Avro GenericRecord
  //   - magnolify-bigquery:  case class <-> BigQuery TableRow
  //   - magnolify-protobuf: case class <-> Protobuf Message
  // Here we show the simpler magnolify-cats module for learning purposes.
  // ---------------------------------------------------------------------------
  def magnolifyCatsDemo(): Unit = {
    println("\n=== Example 6: Magnolify Cats type class derivation ===")

    val strawberry = Plant("🍓", "Strawberry", "perennial")
    val strawberryCopy = Plant("🍓", "Strawberry", "perennial")
    val carrot = Plant("🥕", "Carrot", "biennial")

    // Eq[Plant] — derived by Magnolify, combines Eq[String] for each field.
    // Uses Cats === syntax (type-safe equality, won't compile for mismatched types)
    println(s"strawberry === strawberryCopy: ${strawberry === strawberryCopy}") // true
    println(s"strawberry === carrot: ${strawberry === carrot}")                 // false

    // Show[Plant] — derived by Magnolify, produces a structured string representation
    // Unlike toString, Show is a lawful type class with consistent behavior
    println(s"Show[Plant]: ${strawberry.show}")

    // Neq (=!= in Cats) also works automatically since Eq is derived
    println(s"strawberry =!= carrot: ${strawberry =!= carrot}") // true
  }
}
