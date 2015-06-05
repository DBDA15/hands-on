package de.hpi.fgis;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.hpi.fgis.functions.CreateCells;
import de.hpi.fgis.functions.CreateIndEvidences;
import de.hpi.fgis.functions.FilterEmptyIndSets;
import de.hpi.fgis.functions.MergeCells;
import de.hpi.fgis.functions.MergeIndEvidences;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class Sindy {

	private final Parameters parameters;

	public static void main(String[] args) throws Exception {
		Sindy sindy = new Sindy(args);
		sindy.run();
	}

	public Sindy(String[] args) {
		this.parameters = Parameters.parse(args);
	}

	private void run() throws Exception {
		// Load the execution environment.
		final ExecutionEnvironment env = createExecutionEnvironment();

		// Read and parse the input files.
		Collection<String> inputPaths = loadInputPaths();
		DataSet<Tuple2<int[], String>> cells = null;
		int cellIndexOffset = 0;
		for (String path : inputPaths) {
			DataSource<String> lines = env.readTextFile(path).name("Load " + path);
			DataSet<Tuple2<int[], String>> fileCells = lines
					.flatMap(new CreateCells(';', cellIndexOffset))
					.name("Parse " + path);
			if (cells == null) {
				cells = fileCells;
			} else {
				cells = cells.union(fileCells);
			}

			cellIndexOffset += 1000;
		}

		// Join the cells and keep the attribute groups.
		DataSet<Tuple1<int[]>> attributeGroups = cells
				.groupBy(1).reduceGroup(new MergeCells())
				.name("Merge cells")
				.project(0);

		// Create IND evidences from the attribute groups.
		DataSet<Tuple2<Integer, int[]>> indEvidences = attributeGroups.flatMap(new CreateIndEvidences());

		// Merge the IND evidences to actual INDs.
		DataSet<Tuple2<Integer, int[]>> inds = indEvidences
				.groupBy(0).reduceGroup(new MergeIndEvidences())
				.name("Merge IND evidences")
				.filter(new FilterEmptyIndSets())
				.name("Filter empty IND sets");

		inds.print();

		// Execute the job.
		long startTime = System.currentTimeMillis();
		env.execute("SINDY");
		long endTime = System.currentTimeMillis();

		System.out.format("Exection finished after %.3f s.", (endTime - startTime) / 1000d);
	}

	/**
	 * Creates a execution environment as specified by the parameters.
	 */
	private ExecutionEnvironment createExecutionEnvironment() {
		if (this.parameters.executor != null) {
			final String[] hostAndPort = this.parameters.executor.split(":");
			final String host = hostAndPort[0];
			final int port = Integer.parseInt(hostAndPort[1]);
			if (this.parameters.jars == null || this.parameters.jars.isEmpty()) {
				throw new IllegalStateException("No jars specified to be deployed for remote execution.");
			}
			final String[] jars = new String[this.parameters.jars.size()];
			this.parameters.jars.toArray(jars);
			return ExecutionEnvironment.createRemoteEnvironment(host, port, jars);
		}

		return ExecutionEnvironment.getExecutionEnvironment();
	}

	/**
	 * Collect the input paths from the parameters and expand path patterns.
	 */
	private Collection<String> loadInputPaths() {
		try {
			Collection<String> allInputPaths = new LinkedList<String>();
			for (String rawInputPath : this.parameters.inputFiles) {
				if (rawInputPath.contains("*")) {
					// If the last path of the pattern contains an asterisk, expand the path.
					// Check that the asterisk is contained in the last path segment.
					int lastSlashPos = rawInputPath.lastIndexOf('/');
					if (rawInputPath.indexOf('*') < lastSlashPos) {
						throw new RuntimeException("Path expansion is only possible on the last path segment: " + rawInputPath);
					}

					// Collect all children of the to-be-expanded path.
					String lastSegmentRegex = rawInputPath.substring(lastSlashPos + 1)
							.replace(".", "\\.")
							.replace("[", "\\[")
							.replace("]", "\\]")
							.replace("(", "\\(")
							.replace(")", "\\)")
							.replace("*", ".*");
					Path parentPath = new Path(rawInputPath.substring(0, lastSlashPos));
					FileSystem fs = parentPath.getFileSystem();
					for (FileStatus candidate : fs.listStatus(parentPath)) {
						if (candidate.getPath().getName().matches(lastSegmentRegex)) {
							allInputPaths.add(candidate.getPath().toString());
						}
					}

				} else {
					// Simply add normal paths.
					allInputPaths.add(rawInputPath);
				}
			}
			return allInputPaths;

		} catch (IOException e) {
			throw new RuntimeException("Could not expand paths.", e);
		}
	}

	/**
	 * Parameters for SINDY.
	 */
	private static class Parameters {

		/**
		 * Create parameters from the given command line.
		 */
		static Parameters parse(String... args) {
			try {
				Parameters parameters = new Parameters();
				new JCommander(parameters, args);
				return parameters;
			} catch (final ParameterException e) {
				System.err.println(e.getMessage());
				StringBuilder sb = new StringBuilder();
				new JCommander(new Parameters()).usage(sb);
				for (String line : sb.toString().split("\n")) {
					System.out.println(line);
				}
				System.exit(1);
				return null;
			}
		}

		@Parameter(description = "input CSV files")
		public List<String> inputFiles = new ArrayList<String>();

		@Parameter(names = "--parallelism", description = "degree of parallelism for the job execution")
		public int parallelism = -1;

		@Parameter(names = "--jars", description = "set of jars that are relevant to the execution of SINDY")
		public List<String> jars = null;

		@Parameter(names = "--executor", description = "<host name>:<port> of the Flink cluster")
		public String executor = null;
	}
}
