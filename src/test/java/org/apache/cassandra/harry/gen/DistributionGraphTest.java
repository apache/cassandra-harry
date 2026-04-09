/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.harry.gen;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.imageio.ImageIO;

import org.junit.Test;

/**
 * Generates histogram PNG images for each distribution to visually demonstrate
 * where values are generated. Output goes to build/distribution-graphs/.
 */
public class DistributionGraphTest
{
    private static final long SEED = 42L;
    private static final int SAMPLE_COUNT = 500_000;
    private static final long MIN = 0;
    private static final long MAX = 9999;
    private static final int BINS = 100;

    private static final String OUTPUT_DIR = "build/distribution-graphs";

    // Graph dimensions
    private static final int WIDTH = 900;
    private static final int HEIGHT = 400;
    private static final int MARGIN_LEFT = 70;
    private static final int MARGIN_RIGHT = 30;
    private static final int MARGIN_TOP = 40;
    private static final int MARGIN_BOTTOM = 50;

    @Test
    public void generateAllGraphs() throws IOException
    {
        File dir = new File(OUTPUT_DIR);
        dir.mkdirs();

        Map<String, Distribution> distributions = new LinkedHashMap<>();
        distributions.put("01-uniform", Distribution.uniform(MIN, MAX));
        distributions.put("02-fixed-5000", Distribution.fixed(5000));
        distributions.put("03-gaussian-default", Distribution.gaussian(MIN, MAX));
        distributions.put("04-gaussian-tight-6", Distribution.gaussian(MIN, MAX, 6));
        distributions.put("05-exponential", Distribution.exponential(MIN, MAX));
        distributions.put("06-extreme-shape-0.5", Distribution.extreme(MIN, MAX, 0.5));
        distributions.put("07-extreme-shape-2", Distribution.extreme(MIN, MAX, 2));
        distributions.put("08-extreme-shape-5", Distribution.extreme(MIN, MAX, 5));
        distributions.put("09-quantized-extreme-shape-2-buckets-10", Distribution.quantizedExtreme(MIN, MAX, 2, 10));
        distributions.put("10-zipfian-s-1.0", Distribution.zipfian(MIN, MAX, 1.0));
        distributions.put("11-zipfian-s-0.5", Distribution.zipfian(MIN, MAX, 0.5));
        distributions.put("12-hotspot-0.2-0.8", Distribution.hotspot(MIN, MAX, 0.2, 0.8));
        distributions.put("13-hotspot-0.1-0.95", Distribution.hotspot(MIN, MAX, 0.1, 0.95));
        distributions.put("14-inverted-exponential", Distribution.invert(Distribution.exponential(MIN, MAX)));
        distributions.put("15-gaussian-off-center", Distribution.gaussian(MIN, MAX, 2000, 1000));
        distributions.put("16-inverted-gaussian-off-center", Distribution.invert(Distribution.gaussian(MIN, MAX, 2000, 1000)));
        distributions.put("17-sequential", Distribution.sequential(MIN, MAX));
        distributions.put("18-weighted-1-2-10-20-100-1k-10k", Distribution.weighted(MIN, MAX, 1, 2, 10, 20, 100, 1000, 10000));
        distributions.put("19-weighted-equal", Distribution.weighted(MIN, MAX, 1, 1, 1, 1, 1));

        for (Map.Entry<String, Distribution> entry : distributions.entrySet())
        {
            String name = entry.getKey();
            Distribution dist = entry.getValue();
            long[] histogram = sample(dist, SAMPLE_COUNT);
            long[] binned = bin(histogram, BINS);
            renderGraph(name, binned, new File(dir, name + ".png"));
            System.out.println("Generated: " + name + ".png");
        }

        // Also generate a combined comparison image
        renderComparison(distributions, dir);
        System.out.println("Generated: comparison.png");
    }

    private static long[] sample(Distribution dist, int count)
    {
        EntropySource rng = EntropySource.forTests(SEED);
        long range = dist.max() - dist.min() + 1;
        long[] histogram = new long[(int) range];
        for (int i = 0; i < count; i++)
        {
            long v = dist.next(rng);
            histogram[(int) (v - dist.min())]++;
        }
        return histogram;
    }

    private static long[] bin(long[] histogram, int bins)
    {
        if (histogram.length <= 1)
        {
            long[] binned = new long[bins];
            if (histogram.length == 1)
                binned[bins / 2] = histogram[0];
            return binned;
        }
        long[] binned = new long[bins];
        for (int i = 0; i < histogram.length; i++)
        {
            int b = (int) ((long) i * bins / histogram.length);
            if (b >= bins) b = bins - 1;
            binned[b] += histogram[i];
        }
        return binned;
    }

    private static void renderGraph(String title, long[] bins, File outputFile) throws IOException
    {
        BufferedImage img = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

        // Background
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, WIDTH, HEIGHT);

        int plotW = WIDTH - MARGIN_LEFT - MARGIN_RIGHT;
        int plotH = HEIGHT - MARGIN_TOP - MARGIN_BOTTOM;

        // Find max for scaling
        long maxCount = 0;
        for (long b : bins)
            maxCount = Math.max(maxCount, b);
        if (maxCount == 0) maxCount = 1;

        // Draw grid lines
        g.setColor(new Color(230, 230, 230));
        for (int i = 0; i <= 4; i++)
        {
            int y = MARGIN_TOP + plotH - (plotH * i / 4);
            g.drawLine(MARGIN_LEFT, y, MARGIN_LEFT + plotW, y);
        }

        // Draw bars
        g.setColor(new Color(70, 130, 180));
        int barWidth = Math.max(1, plotW / bins.length);
        for (int i = 0; i < bins.length; i++)
        {
            int barH = (int) (plotH * bins[i] / maxCount);
            int x = MARGIN_LEFT + (plotW * i / bins.length);
            int y = MARGIN_TOP + plotH - barH;
            g.fillRect(x, y, barWidth, barH);
        }

        // Axes
        g.setColor(Color.BLACK);
        g.setStroke(new BasicStroke(1.5f));
        g.drawLine(MARGIN_LEFT, MARGIN_TOP, MARGIN_LEFT, MARGIN_TOP + plotH);
        g.drawLine(MARGIN_LEFT, MARGIN_TOP + plotH, MARGIN_LEFT + plotW, MARGIN_TOP + plotH);

        // Labels
        g.setFont(new Font("SansSerif", Font.BOLD, 14));
        g.drawString(title, MARGIN_LEFT, MARGIN_TOP - 12);

        g.setFont(new Font("SansSerif", Font.PLAIN, 11));
        // Y-axis labels
        for (int i = 0; i <= 4; i++)
        {
            int y = MARGIN_TOP + plotH - (plotH * i / 4);
            long val = maxCount * i / 4;
            String label = formatCount(val);
            g.drawString(label, 5, y + 4);
        }

        // X-axis labels
        g.drawString("min", MARGIN_LEFT, MARGIN_TOP + plotH + 18);
        g.drawString("max", MARGIN_LEFT + plotW - 20, MARGIN_TOP + plotH + 18);
        g.drawString("mid", MARGIN_LEFT + plotW / 2 - 10, MARGIN_TOP + plotH + 18);

        // X-axis title
        g.setFont(new Font("SansSerif", Font.PLAIN, 11));
        g.drawString("Value range [0, 9999]", MARGIN_LEFT + plotW / 2 - 50, MARGIN_TOP + plotH + 38);

        g.dispose();
        ImageIO.write(img, "PNG", outputFile);
    }

    private static void renderComparison(Map<String, Distribution> distributions, File dir) throws IOException
    {
        // Select a subset for the comparison
        String[] compareNames = {
            "01-uniform", "03-gaussian-default", "05-exponential",
            "06-extreme-shape-0.5", "10-zipfian-s-1.0", "12-hotspot-0.2-0.8"
        };
        Color[] colors = {
            new Color(70, 130, 180),   // steel blue
            new Color(220, 50, 50),    // red
            new Color(50, 180, 50),    // green
            new Color(180, 100, 220),  // purple
            new Color(255, 140, 0),    // orange
            new Color(0, 180, 180),    // teal
        };

        int compW = 1000, compH = 500;
        int mLeft = 70, mRight = 200, mTop = 40, mBot = 50;
        int plotW = compW - mLeft - mRight;
        int plotH = compH - mTop - mBot;

        BufferedImage img = new BufferedImage(compW, compH, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

        g.setColor(Color.WHITE);
        g.fillRect(0, 0, compW, compH);

        // Compute all binned data and find global max
        long[][] allBins = new long[compareNames.length][];
        long globalMax = 0;
        for (int d = 0; d < compareNames.length; d++)
        {
            Distribution dist = distributions.get(compareNames[d]);
            long[] hist = sample(dist, SAMPLE_COUNT);
            allBins[d] = bin(hist, BINS);
            for (long b : allBins[d])
                globalMax = Math.max(globalMax, b);
        }
        if (globalMax == 0) globalMax = 1;

        // Grid
        g.setColor(new Color(230, 230, 230));
        for (int i = 0; i <= 4; i++)
        {
            int y = mTop + plotH - (plotH * i / 4);
            g.drawLine(mLeft, y, mLeft + plotW, y);
        }

        // Draw lines for each distribution
        g.setStroke(new BasicStroke(2.0f));
        for (int d = 0; d < compareNames.length; d++)
        {
            g.setColor(colors[d]);
            long[] bins = allBins[d];
            int prevX = -1, prevY = -1;
            for (int i = 0; i < bins.length; i++)
            {
                int x = mLeft + (plotW * i / bins.length) + (plotW / bins.length / 2);
                int y = mTop + plotH - (int) (plotH * bins[i] / globalMax);
                if (prevX >= 0)
                    g.drawLine(prevX, prevY, x, y);
                prevX = x;
                prevY = y;
            }
        }

        // Axes
        g.setColor(Color.BLACK);
        g.setStroke(new BasicStroke(1.5f));
        g.drawLine(mLeft, mTop, mLeft, mTop + plotH);
        g.drawLine(mLeft, mTop + plotH, mLeft + plotW, mTop + plotH);

        // Title
        g.setFont(new Font("SansSerif", Font.BOLD, 14));
        g.drawString("Distribution Comparison", mLeft, mTop - 12);

        // Y-axis
        g.setFont(new Font("SansSerif", Font.PLAIN, 11));
        for (int i = 0; i <= 4; i++)
        {
            int y = mTop + plotH - (plotH * i / 4);
            g.drawString(formatCount(globalMax * i / 4), 5, y + 4);
        }

        // X-axis
        g.drawString("min", mLeft, mTop + plotH + 18);
        g.drawString("max", mLeft + plotW - 20, mTop + plotH + 18);

        // Legend
        g.setFont(new Font("SansSerif", Font.PLAIN, 12));
        int legendX = mLeft + plotW + 15;
        int legendY = mTop + 20;
        for (int d = 0; d < compareNames.length; d++)
        {
            g.setColor(colors[d]);
            g.fillRect(legendX, legendY - 10, 14, 14);
            g.setColor(Color.BLACK);
            String label = compareNames[d].substring(3); // strip numbering prefix
            g.drawString(label, legendX + 20, legendY + 2);
            legendY += 22;
        }

        g.dispose();
        ImageIO.write(img, "PNG", new File(dir, "comparison.png"));
    }

    private static String formatCount(long count)
    {
        if (count >= 1000)
            return String.format("%.1fk", count / 1000.0);
        return String.valueOf(count);
    }
}
