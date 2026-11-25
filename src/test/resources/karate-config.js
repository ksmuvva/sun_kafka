// Runs before every scenario. baseUrl is the random port of the app started by KarateTest.
function fn() {
  return {
    baseUrl: karate.properties['karate.baseUrl']
  };
}
