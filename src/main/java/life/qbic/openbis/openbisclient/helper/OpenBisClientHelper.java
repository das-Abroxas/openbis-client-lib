package life.qbic.openbis.openbisclient.helper;

import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.fetchoptions.DataSetFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.fetchoptions.ExperimentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.fetchoptions.ExperimentTypeFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.fetchoptions.PersonFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.fetchoptions.ProjectFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.PropertyAssignment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.fetchoptions.PropertyAssignmentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.fetchoptions.PropertyTypeFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.fetchoptions.RoleAssignmentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions.SampleFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions.SampleTypeFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.fetchoptions.SpaceFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.fetchoptions.VocabularyFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.fetchoptions.VocabularyTermFetchOptions;

public class OpenBisClientHelper {

  public static PersonFetchOptions fetchPersonCompletely() {
    PersonFetchOptions personFetchOptions = new PersonFetchOptions();
    personFetchOptions.withRoleAssignments();
    personFetchOptions.withSpace();
    personFetchOptions.withRegistrator();

    return personFetchOptions;
  }

  public static PersonFetchOptions fetchPersonWithSpace() {
    PersonFetchOptions personFetchOptions = new PersonFetchOptions();
    personFetchOptions.withSpaceUsing(fetchSpacesCompletely());

    return personFetchOptions;
  }


  public static SpaceFetchOptions fetchSpacesCompletely() {
    SpaceFetchOptions spaceFetchOptions = new SpaceFetchOptions();
    spaceFetchOptions.withProjects();
    spaceFetchOptions.withRegistrator();
    spaceFetchOptions.withSamples();

    return spaceFetchOptions;
  }

  public static SpaceFetchOptions fetchSpacesWithSamples() {
    SpaceFetchOptions spaceFetchOptions = new SpaceFetchOptions();
    spaceFetchOptions.withSamplesUsing(fetchSamplesCompletely());

    return spaceFetchOptions;
  }


  public static SampleFetchOptions fetchSamplesCompletely() {
    SampleFetchOptions sampleFetchOptions = new SampleFetchOptions();
    sampleFetchOptions.withExperiment();
    sampleFetchOptions.withAttachments();
    sampleFetchOptions.withComponents();
    sampleFetchOptions.withContainer();
    sampleFetchOptions.withDataSets();
    sampleFetchOptions.withHistory();
    sampleFetchOptions.withMaterialProperties();
    sampleFetchOptions.withModifier();
    sampleFetchOptions.withProject();
    sampleFetchOptions.withProperties();
    sampleFetchOptions.withRegistrator();
    sampleFetchOptions.withSpace();
    sampleFetchOptions.withTags();
    sampleFetchOptions.withType();
    sampleFetchOptions.withChildren();
    sampleFetchOptions.withParents();

    return sampleFetchOptions;
  }

  public static SampleFetchOptions fetchSamplesWithParentsAndChildrenCompletely() {
    SampleFetchOptions sampleFetchOptions = new SampleFetchOptions();
    sampleFetchOptions.withExperiment();
    sampleFetchOptions.withAttachments();
    sampleFetchOptions.withComponents();
    sampleFetchOptions.withContainer();
    sampleFetchOptions.withDataSets();
    sampleFetchOptions.withHistory();
    sampleFetchOptions.withMaterialProperties();
    sampleFetchOptions.withModifier();
    sampleFetchOptions.withProject();
    sampleFetchOptions.withProperties();
    sampleFetchOptions.withRegistrator();
    sampleFetchOptions.withSpace();
    sampleFetchOptions.withTags();
    sampleFetchOptions.withType();
    sampleFetchOptions.withChildrenUsing(fetchSamplesCompletely());
    sampleFetchOptions.withParentsUsing(fetchSamplesCompletely());

    return sampleFetchOptions;
  }

  public static SampleFetchOptions fetchSamplesWithDataSets() {
    SampleFetchOptions sampleFetchOptions = new SampleFetchOptions();
    sampleFetchOptions.withDataSetsUsing(fetchDataSetsCompletely());

    return sampleFetchOptions;
  }


  public static ProjectFetchOptions fetchProjectsCompletely() {
    ProjectFetchOptions projectFetchOptions = new ProjectFetchOptions();
    projectFetchOptions.withAttachments();
    projectFetchOptions.withHistory();
    projectFetchOptions.withModifier();
    projectFetchOptions.withRegistrator();
    projectFetchOptions.withSpace();
    projectFetchOptions.withExperiments();
    projectFetchOptions.withLeader();
    projectFetchOptions.withSamples();
    projectFetchOptions.withSpace();

    return projectFetchOptions;
  }

  public static ProjectFetchOptions fetchProjectsWithSamples() {
    ProjectFetchOptions projectFetchOptions = new ProjectFetchOptions();
    projectFetchOptions.withSamplesUsing(fetchSamplesCompletely());

    return projectFetchOptions;
  }

  public static ProjectFetchOptions fetchProjectsWithSamplesAndDataSets() {
    ProjectFetchOptions projectFetchOptions = new ProjectFetchOptions();
    projectFetchOptions.withSamplesUsing(fetchSamplesWithDataSets());

    return projectFetchOptions;
  }


  public static ExperimentFetchOptions fetchExperimentsCompletely() {
    ExperimentFetchOptions experimentFetchOptions = new ExperimentFetchOptions();
    experimentFetchOptions.withProject();
    experimentFetchOptions.withSamples();
    experimentFetchOptions.withDataSets();
    experimentFetchOptions.withAttachments();
    experimentFetchOptions.withHistory();
    experimentFetchOptions.withModifier();
    experimentFetchOptions.withRegistrator();
    experimentFetchOptions.withMaterialProperties();
    experimentFetchOptions.withProperties();
    experimentFetchOptions.withTags();
    experimentFetchOptions.withType();

    return experimentFetchOptions;
  }

  public static ExperimentFetchOptions fetchExperimentsWithSamples() {
    ExperimentFetchOptions experimentFetchOptions = new ExperimentFetchOptions();
    experimentFetchOptions.withSamplesUsing(fetchSamplesCompletely());

    return experimentFetchOptions;
  }

  public static ExperimentFetchOptions fetchExperimentsWithDataSets() {
    ExperimentFetchOptions experimentFetchOptions = new ExperimentFetchOptions();
    experimentFetchOptions.withDataSetsUsing(fetchDataSetsCompletely());

    return experimentFetchOptions;
  }


  public static DataSetFetchOptions fetchDataSetsCompletely() {
    DataSetFetchOptions dataSetFetchOptions = new DataSetFetchOptions();
    dataSetFetchOptions.withProperties();
    dataSetFetchOptions.withChildren();
    dataSetFetchOptions.withComponents();
    dataSetFetchOptions.withContainers();
    dataSetFetchOptions.withDataStore();
    dataSetFetchOptions.withExperiment();
    dataSetFetchOptions.withHistory();
    dataSetFetchOptions.withLinkedData();
    dataSetFetchOptions.withMaterialProperties();
    dataSetFetchOptions.withModifier();
    dataSetFetchOptions.withParents();
    dataSetFetchOptions.withPhysicalData();
    dataSetFetchOptions.withRegistrator();
    dataSetFetchOptions.withSample();
    dataSetFetchOptions.withTags();
    dataSetFetchOptions.withType();

    return dataSetFetchOptions;
  }

  public static DataSetFetchOptions fetchDataSetsWithSampleCompletely() {
    DataSetFetchOptions dataSetFetchOptions = new DataSetFetchOptions();
    dataSetFetchOptions.withSampleUsing(fetchSamplesCompletely());
    dataSetFetchOptions.withProperties();
    dataSetFetchOptions.withChildren();
    dataSetFetchOptions.withComponents();
    dataSetFetchOptions.withContainers();
    dataSetFetchOptions.withDataStore();
    dataSetFetchOptions.withExperiment();
    dataSetFetchOptions.withHistory();
    dataSetFetchOptions.withLinkedData();
    dataSetFetchOptions.withMaterialProperties();
    dataSetFetchOptions.withModifier();
    dataSetFetchOptions.withParents();
    dataSetFetchOptions.withPhysicalData();
    dataSetFetchOptions.withRegistrator();
    dataSetFetchOptions.withTags();
    dataSetFetchOptions.withType();

    return dataSetFetchOptions;
  }


  public static SampleTypeFetchOptions fetchSampleTypesCompletely() {
    SampleTypeFetchOptions sampleTypeFetchOption = new SampleTypeFetchOptions();
    sampleTypeFetchOption.withPropertyAssignments();

    return sampleTypeFetchOption;
  }

  public static ExperimentTypeFetchOptions fetchExperimentTypesCompletely() {
    ExperimentTypeFetchOptions experimentTypeFetchOptions = new ExperimentTypeFetchOptions();
    experimentTypeFetchOptions.withPropertyAssignments();

    return experimentTypeFetchOptions;
  }


  public static VocabularyFetchOptions fetchVocabularyCompletely() {
    VocabularyFetchOptions vocabularyFetchOptions = new VocabularyFetchOptions();
    vocabularyFetchOptions.withRegistrator();
    vocabularyFetchOptions.withTerms();

    return vocabularyFetchOptions;
  }

  public static VocabularyTermFetchOptions fetchVocabularyTermCompletely() {
    VocabularyTermFetchOptions vocabularyTermFetchOptions = new VocabularyTermFetchOptions();
    vocabularyTermFetchOptions.withVocabulary();
    vocabularyTermFetchOptions.withRegistrator();

    return vocabularyTermFetchOptions;
  }


  public static PropertyAssignmentFetchOptions fetchPropertyAssignmentCompletely() {
    PropertyAssignmentFetchOptions propertyAssignmentFetchOptions = new PropertyAssignmentFetchOptions();
    propertyAssignmentFetchOptions.withPropertyType();
    propertyAssignmentFetchOptions.withEntityType();
    propertyAssignmentFetchOptions.withPlugin();
    propertyAssignmentFetchOptions.withRegistrator();

    return propertyAssignmentFetchOptions;
  }

  public static PropertyAssignmentFetchOptions fetchPropertyAssignmentWithPropertyType() {
    PropertyAssignmentFetchOptions propertyAssignmentFetchOptions = new PropertyAssignmentFetchOptions();
    propertyAssignmentFetchOptions.withPropertyTypeUsing(fetchPropertyTypeCompletely());

    return propertyAssignmentFetchOptions;
  }

  public static PropertyTypeFetchOptions fetchPropertyTypeCompletely() {
    PropertyTypeFetchOptions propertyTypeFetchOptions = new PropertyTypeFetchOptions();
    propertyTypeFetchOptions.withVocabulary();
    propertyTypeFetchOptions.withSemanticAnnotations();
    propertyTypeFetchOptions.withMaterialType();
    propertyTypeFetchOptions.withRegistrator();

    return propertyTypeFetchOptions;
  }

  public static PropertyTypeFetchOptions fetchPropertyTypeWithVocabularyAndTerms() {
    PropertyTypeFetchOptions propertyTypeFetchOptions = new PropertyTypeFetchOptions();
    propertyTypeFetchOptions.withVocabularyUsing(fetchVocabularyCompletely());

    return propertyTypeFetchOptions;
  }


  public static RoleAssignmentFetchOptions fetchRoleAssignmentCompletely() {
    RoleAssignmentFetchOptions roleAssignmentFetchOptions = new RoleAssignmentFetchOptions();
    roleAssignmentFetchOptions.withUser();
    roleAssignmentFetchOptions.withSpace();
    roleAssignmentFetchOptions.withProject();
    roleAssignmentFetchOptions.withRegistrator();
    roleAssignmentFetchOptions.withAuthorizationGroup();

    return roleAssignmentFetchOptions;
  }

  public static RoleAssignmentFetchOptions fetchRoleAssignmentWithSpaceAndUser() {
    RoleAssignmentFetchOptions roleAssignmentFetchOptions = new RoleAssignmentFetchOptions();
    roleAssignmentFetchOptions.withUser();
    roleAssignmentFetchOptions.withSpace();

    return roleAssignmentFetchOptions;
  }
}
