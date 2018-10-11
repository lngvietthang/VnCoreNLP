package vn.pipeline;

import marmot.util.Sys;
import org.apache.log4j.Logger;
import vn.corenlp.ner.NerRecognizer;
import vn.corenlp.parser.DependencyParser;
import vn.corenlp.postagger.PosTagger;
import vn.corenlp.tokenizer.Tokenizer;
import vn.corenlp.wordsegmenter.WordSegmenter;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

import java.io.*;
import java.security.spec.InvalidParameterSpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class VnCoreNLP {

    public final static Logger LOGGER = Logger.getLogger(Annotation.class);

    private WordSegmenter wordSegmenter;
    private PosTagger posTagger;
    private NerRecognizer nerRecognizer;
    private DependencyParser dependencyParser;

    private final static String              WORDSEGMENTER          = "wseg";
    private final static String              POSTAGGER              = "pos";
    private final static String              NERRECOGNIZER          = "ner";
    private final static String              DEPENDENCYPARSER       = "parse";
    private final static List<String> DEFAULT_ANNOTATORS = Arrays.asList(WORDSEGMENTER, POSTAGGER, NERRECOGNIZER, DEPENDENCYPARSER);
    private final static List<String> FORMAT_OPTIONS = Arrays.asList("column", "inline");
    private final static String DEFAULT_FORMAT = "inline";

    public VnCoreNLP() throws IOException {
        List<String> annotators = DEFAULT_ANNOTATORS;
        initAnnotators(annotators);
    }

    public VnCoreNLP(List<String> annotators) throws IOException {
        initAnnotators(annotators);

    }

    public void initAnnotators(List<String> annotators) throws IOException{
        for(String annotator : annotators) {
            switch (annotator.trim()) {
                case "parse":
                    this.dependencyParser = DependencyParser.initialize();
                    break;
                case "ner":
                    this.nerRecognizer = NerRecognizer.initialize();
                    break;
                case "pos":
                    this.posTagger = PosTagger.initialize();
                    break;
                case "wseg":
                    this.wordSegmenter = WordSegmenter.initialize();
                    break;
            }
        }

    }

    public void printToFile(Annotation annotation, PrintStream printer) throws IOException {
        for(Sentence sentence : annotation.getSentences()) {
            printer.println(sentence.toString());
        }
    }

    public void printToFile(Annotation annotation, String fileOut) throws IOException {
        PrintStream printer = new PrintStream(fileOut, "UTF-8");
        for(Sentence sentence : annotation.getSentences()) {
            printer.println(sentence.toString() + "\n");
        }
    }

    private static List<File> listf(String directoryName) {
        File directory = new File(directoryName);
        List<File> resultList = new ArrayList<File>();

//        get all the files from a directory
        File[] fList = directory.listFiles();
        for (File file : fList) {
            if (file.isFile()) {
//                Check hidden file of macos
                if(!file.getName().equals(".DS_Store")) {
                    resultList.add(file);
                }
            } else if (file.isDirectory()) {
                resultList.addAll(listf(file.getAbsolutePath()));
            }
        }
        return resultList;
    }

    public void annotate(Annotation annotation) throws IOException {
        List<String> rawSentences = Tokenizer.joinSentences(Tokenizer.tokenize(annotation.getRawText()));
        annotation.setSentences(new ArrayList<>());
        for (String rawSentence : rawSentences) {
            if (rawSentence.trim().length() > 0) {
                Sentence sentence = new Sentence(rawSentence, wordSegmenter, posTagger, nerRecognizer, dependencyParser);
                annotation.getSentences().add(sentence);
                annotation.getTokens().addAll(sentence.getTokens());
                annotation.getWords().addAll(sentence.getWords());
                annotation.setWordSegmentedText(annotation.getWordSegmentedTaggedText() + sentence.getWordSegmentedSentence() + " ");
            }

        }

        annotation.setWordSegmentedText(annotation.getWordSegmentedTaggedText().trim());

    }

    public static void printUsage(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("java -Xmx2g -jar VnCoreNLP.jar [Options]", options);
        System.out.println("Example:");
        System.out.println("With file:");
        System.out.println("java -Xmx2g -jar VnCoreNLP.jar -fin sample_input.txt -fout output.txt");
        System.out.println("java -Xmx2g -jar VnCoreNLP.jar -format column -fin sample_input.txt -fout output.txt");
        System.out.println("java -Xmx2g -jar VnCoreNLP.jar -format column -annotator wseg,pos,ner -fin sample_input.txt -fout output.txt");
        System.out.println("With directory:");
        System.out.println("java -Xmx2g -jar VnCoreNLP.jar -din sample_input/ -dout output/");
        System.out.println("java -Xmx2g -jar VnCoreNLP.jar -format column -din sample_input/ -dout output/");
        System.out.println("java -Xmx2g -jar VnCoreNLP.jar -format column -annotator wseg,pos,ner -din sample_input/ -dout output/");

    }

    private static Options buildOptions() {
        Options options = new Options();
        Option help = Option.builder("h").longOpt("help").desc("Show help infomation").build();
        Option format = Option.builder("f").longOpt("format").hasArg()
                .desc(String.format("Output format: column or inline (optional, default: %s)", DEFAULT_FORMAT))
                .build();
        Option fin = Option.builder().longOpt("fin").hasArg().argName("FILE").desc("Path to input file").build();
        Option fout = Option.builder().longOpt("fout").hasArg().argName("FILE").desc("Path to output file (optional, default: fin-name.out)").build();
        Option din = Option.builder().longOpt("din").hasArg().argName("DIR").desc("Path to input directory").build();
        Option dout = Option.builder().longOpt("dout").hasArg().argName("DIR").desc("Path to output directory (optional, default: din/*.out)").build();
        Option annotators = Option.builder("a").longOpt("annotators").hasArg().argName("annotations")
                .desc(String.format("The annotators to run over a given sentence (optional, default: \"%s\")",
                        String.join(",", DEFAULT_ANNOTATORS)))
                .build();

        options.addOption(help);
        options.addOption(fin);
        options.addOption(fout);
        options.addOption(din);
        options.addOption(dout);
        options.addOption(format);
        options.addOption(annotators);

        return options;
    }

    public static void processPipeline(String fileIn, String fileOut, List<String> annotators, String format) throws IOException{

        FileInputStream fis = new FileInputStream(new File(fileIn));
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(fileOut)), "UTF-8");

        BufferedReader br = new BufferedReader(isr);
        VnCoreNLP pipeline = new VnCoreNLP(annotators);
        LOGGER.info("Start processing " + fileIn);
        while(br.ready()) {
            String line = br.readLine();
            if (line.trim().length() > 0) {
                Annotation annotation = new Annotation(line);
                pipeline.annotate(annotation);
                osw.write(annotation.toString(format));
            }
        }
        br.close();
        isr.close();
        fis.close();
        osw.close();
        LOGGER.info("Wrote output to " +  fileOut);
    }

    public static void processPipelineWithDir(String dirIn, String dirOut, List<String> annotators, String format) throws IOException{

        List<File> resultList = listf(dirIn);
        for(File inFile: resultList) {
            File outFile = new File(dirOut + "/" + inFile.getName() + ".out");
            processPipeline(inFile.getPath(), outFile.getPath(), annotators, format);
        }
    }

    public static void main(String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine commandLine;
        Options options = buildOptions();

        try {
            commandLine = commandLineParser.parse(options, args);

            if (args.length == 0 || commandLine.hasOption("help")) {
                printUsage(options);
            }
            else {
//                Check format
                String cmdFormat = commandLine.getOptionValue("format", DEFAULT_FORMAT).toLowerCase().trim();
                if (!FORMAT_OPTIONS.contains(cmdFormat)) {
                    throw new InvalidParameterSpecException(String.format("Format \"%s\" is invalid.", cmdFormat));
                }
                String format = cmdFormat;

//                Check annotators
                String[] cmdAnnotators = commandLine.getOptionValue("annotators", String.join(",", DEFAULT_ANNOTATORS))
                        .toLowerCase().trim().split("\\s*,\\s*");
                List<String> annotators = new ArrayList<>();
                for (String annotator : cmdAnnotators) {
                    if (annotator.length() > 0) {
                        if (!DEFAULT_ANNOTATORS.contains(annotator)) {
                            throw new InvalidParameterSpecException(String.format("Annotator \"%s\" is invalid.", annotator));
                        }
                        annotators.add(annotator);
                    }
                }

//                Check input
                String fileIn = commandLine.getOptionValue("fin", null);
                String dirIn = commandLine.getOptionValue("din", null);
                if (fileIn == null && dirIn == null) {
                    System.out.println("Missing input!");
                    printUsage(options);
                    return;
                }
                if (fileIn != null && dirIn != null) {
                    System.out.println("Many inputs!");
                    printUsage(options);
                    return;
                }

                if (fileIn != null) {
                    String fileOut = commandLine.getOptionValue("fout", null);
                    if (fileOut == null) {
                        fileOut = fileIn + ".out";
                    }
//                    Process file
                    processPipeline(fileIn, fileOut, annotators, format);
                }
                else {
                    String dirOut = commandLine.getOptionValue("dout", null);
                    if (dirOut == null) {
                        dirOut = dirIn;
                    }
//                    Process directory
                    processPipelineWithDir(dirIn, dirOut, annotators, format);
                }
            }
        }
        catch (ParseException pe) {
            LOGGER.error(pe.getMessage(), pe);
            printUsage(options);
            System.exit(1);
        }
        catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            System.exit(1);
        }
    }
}
