package org.apache.flink.streaming.paper.experiments;

import org.aeonbits.owner.Config;

/**
 * This is a skeleton interface used by owner to generate an implementation from the given properties file.
 * If you need to use another properties file please change its Sources annotation. For working with multiple
 * source files you can check the following: http://owner.aeonbits.org/docs/loading-strategies/
 */
@Config.LoadPolicy(Config.LoadType.FIRST)
@Config.Sources({"classpath:default.properties","classpath:pairs.properties"})
public interface ExperimentConfig extends Config{


    /**
     * The number of time based queries to be generated
     */
    @DefaultValue("100")
    int numTimeQueries();

    /**
     * The number of count based queries to be generated
     */
    @DefaultValue("100")
    int numCountQueries();

    /**
     * The number of tuples which will be processed in the experiment.
     * Make sure to select a value which is higher than the actual number of tuples covered in the experiment to
     * allow deterministic not periodic policies to compute their look-ahead.
     */
    @DefaultValue("2000000")
    int countExperimentSize();

    /**
     * The maximum timestamp which will be processed in the experiment.
     * Make sure to select a value which is higher than the actual number of tuples covered in the experiment to
     * allow deterministic not periodic policies to compute their look-ahead.
     * The lowest timestamp is assumed to be greater or equal to zero.
     */
    @DefaultValue("2000000")
    int timeExperimentSize();

    /**
     * A scale factor: All the minimums and maximums below will be multiplied with this factor.
     */
    @DefaultValue("10")
    double scaleFactor();

    /**
     * Indicates if window ends for the random walk shall be generated or not.
     */
    @DefaultValue("true")
    boolean generateRandomWalks();

    /*******************************************************
     * COUNT BASED QUERIES SETUP
     *******************************************************/

    @DefaultValue("true")
    boolean scenarioRegularSlideRegularRange();

    @DefaultValue("true")
    boolean scenarioSmallSlideRegularRange();

    @DefaultValue("true")
    boolean scenarioHighSlideRegularRange();

    @DefaultValue("true")
    boolean scenarioRegularSlideSmallRange();

    @DefaultValue("true")
    boolean scenarioRegularSlideHighRange();

    @DefaultValue("true")
    boolean scenarioSmallSlideHighRange();

    @DefaultValue("true")
    boolean scenarioHighSlideHighRange();

    @DefaultValue("true")
    boolean scenarioSmallSlideSmallRange();

    @DefaultValue("true")
    boolean scenarioHighSlideSmallRange();

    /*******************************************************
     * COUNT BASED QUERIES SETUP
     *******************************************************/

    /**
     * Minimum slide step for the regular case (COUNT)
     */
    @DefaultValue("1")
    double regularCountMinSlide();

    /**
     * Maximum slide step for the regular case (COUNT)
     */
    @DefaultValue("9")
    double regularCountMaxSlide();

    /**
     * Minimum slide step for the scenario with small slides (COUNT)
     */
    @DefaultValue("1")
    double lowerCountMinSlide();

    /**
     * Maximum slide step for the scenario with small slides (COUNT)
     */
    @DefaultValue("3")
    double lowerCountMaxSlide();

    /**
     * Minimum slide step for the scenario with large slides (COUNT)
     */
    @DefaultValue("7")
    double upperCountMinSlide();

    /**
     * Maximum slide step for the scenario with large slides (COUNT)
     */
    @DefaultValue("9")
    double upperCountMaxSlide();

    /**
     * Minimum range for the regular case (COUNT)
     */
    @DefaultValue("11")
    double regularCountMinRange();

    /**
     * Maximum range for the regular case (COUNT)
     */
    @DefaultValue("19")
    double regularCountMaxRange();

    /**
     * Minimum range for the scenario with small ranges (COUNT)
     */
    @DefaultValue("11")
    double lowerCountMinRange();

    /**
     * Maximum range for the scenario with small ranges (COUNT)
     */
    @DefaultValue("13")
    double lowerCountMaxRange();

    /**
     * Minimum range for the scenario with large ranges (COUNT)
     */
    @DefaultValue("17")
    double upperCountMinRange();

    /**
     * Maximum range for the scenario with large ranges (COUNT)
     */
    @DefaultValue("19")
    double upperCountMaxRange();


    /*******************************************************
     * TIME BASED QUERIES SETUP
     *******************************************************/

    /**
     * Minimum slide step for the regular case (TIME)
     */
    @DefaultValue("1")
    double regularTimeMinSlide();

    /**
     * Maximum slide step for the regular case (TIME)
     */
    @DefaultValue("9")
    double regularTimeMaxSlide();

    /**
     * Minimum slide step for the scenario with small slides (TIME)
     */
    @DefaultValue("1")
    double lowerTimeMinSlide();

    /**
     * Maximum slide step for the scenario with small slides (TIME)
     */
    @DefaultValue("3")
    double lowerTimeMaxSlide();

    /**
     * Minimum slide step for the scenario with large slides (TIME)
     */
    @DefaultValue("7")
    double upperTimeMinSlide();

    /**
     * Maximum slide step for the scenario with large slides (TIME)
     */
    @DefaultValue("9")
    double upperTimeMaxSlide();

    /**
     * Minimum range for the regular case (TIME)
     */
    @DefaultValue("11")
    double regularTimeMinRange();

    /**
     * Maximum range for the regular case (TIME)
     */
    @DefaultValue("19")
    double regularTimeMaxRange();

    /**
     * Minimum range for the scenario with small ranges (TIME)
     */
    @DefaultValue("11")
    double lowerTimeMinRange();

    /**
     * Maximum range for the scenario with small ranges (TIME)
     */
    @DefaultValue("13")
    double lowerTimeMaxRange();

    /**
     * Minimum range for the scenario with large ranges (TIME)
     */
    @DefaultValue("17")
    double upperTimeMinRange();

    /**
     * Maximum range for the scenario with large ranges (TIME)
     */
    @DefaultValue("19")
    double upperTimeMaxRange();

    
}
